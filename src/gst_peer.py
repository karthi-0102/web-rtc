#!/usr/bin/env python3
"""
Multi-peer WebRTC media server built on GStreamer's webrtcbin.

Exists because @roamhq/wrtc cannot encode H.264, and KVS WebRTC ingestion
requires it. Node keeps AWS auth, discovery and signaling; this process owns
the media: one capture, one H.264 encode, fanned out to N peers through a tee.

That fan-out matters for two reasons: a camera can only be opened once, and
encoding once for ten viewers costs the same as encoding once for one.

Protocol: newline-delimited JSON on stdin/stdout. Every message carries `id`
identifying the peer, except `ready`/`fatal` which are process-wide.

  in : {"type":"add_peer","id":X,"recvAudio":bool}
       {"type":"offer","id":X,"sdp":...}
       {"type":"ice","id":X,"candidate":...,"sdpMLineIndex":N}
       {"type":"remove_peer","id":X}  {"type":"stop"}
  out: {"type":"ready"}  {"type":"fatal","msg":...}
       {"type":"answer","id":X,"sdp":...}
       {"type":"ice","id":X,"candidate":...,"sdpMLineIndex":N}
       {"type":"state","id":X,"connection":...|"ice":...}
       {"type":"log","level":...,"msg":...,"fields":{...}}
       {"type":"preview","jpeg":"<base64>"}   (monitor thumbnail, if enabled)
"""
import base64
import json
import sys
import threading

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstWebRTC', '1.0')
gi.require_version('GstSdp', '1.0')
from gi.repository import Gst, GstWebRTC, GstSdp, GLib  # noqa: E402

Gst.init(None)

# Per-peer send-queue depth. Bounds how much encoded media a slow peer may hold
# before the queue starts leaking, and so bounds the latency it adds.
SEND_QUEUE_NS = 150 * 1_000_000

_out_lock = threading.Lock()


def emit(obj):
    """Write one JSON line to Node. Locked: GLib callbacks run off-thread."""
    with _out_lock:
        sys.stdout.write(json.dumps(obj) + '\n')
        sys.stdout.flush()


def log(level, msg, **fields):
    emit({'type': 'log', 'level': level, 'msg': msg, 'fields': fields})


def preview_branch(cfg):
    """
    A low-rate JPEG copy of the frames being published, for the monitor window.

    Taken off the raw tee before the encoder, so it shows exactly what the
    encoder is fed. `leaky=downstream` and `drop=true` keep a stalled monitor
    from ever back-pressuring the media path.
    """
    fps = cfg.get('previewFps') or 0
    if fps <= 0:
        return ''
    w, h = cfg['width'], cfg['height']
    pw = min(480, w)
    ph = max(2, (round(h * pw / w) // 2) * 2)
    return (
        f'rawtee. ! queue leaky=downstream max-size-buffers=2 ! '
        f'videorate ! video/x-raw,framerate={fps}/1 ! '
        f'videoscale ! video/x-raw,width={pw},height={ph} ! '
        f'jpegenc quality=60 ! '
        f'appsink name=preview emit-signals=true max-buffers=1 drop=true sync=false'
    )


def video_chain(cfg):
    src = cfg['videoInput']
    w, h, fps = cfg['width'], cfg['height'], cfg['fps']
    if src == 'test':
        head = 'videotestsrc is-live=true pattern=smpte'
    elif src.startswith('/dev/video'):
        head = f'v4l2src device={src}'
    elif src == 'screen':
        head = 'ximagesrc use-damage=false'
    else:
        head = f'filesrc location={src} ! decodebin'
    preview = preview_branch(cfg)
    return (
        f'{head} ! videoconvert ! videoscale ! videorate ! '
        f'video/x-raw,width={w},height={h},framerate={fps}/1 ! '
        # Raw fan-out: one branch encodes for the network, the other renders a
        # thumbnail for the monitor window (absent when preview is off).
        f'tee name=rawtee allow-not-linked=true '
        f'rawtee. ! queue ! '
        # KVS ingestion requires H.264. zerolatency plus a 2s GOP so a peer
        # joining mid-stream gets a keyframe quickly.
        f'x264enc name=venc tune=zerolatency speed-preset=veryfast '
        f'bitrate={cfg["videoBitrateKbps"]} key-int-max={fps * 2} ! '
        f'video/x-h264,profile=constrained-baseline ! '
        f'h264parse config-interval=-1 ! '
        # The tee carries encoded H.264, not RTP: each peer packetises with its
        # own payload type (AWS demands 126, browsers pick their own).
        f'tee name=vtee allow-not-linked=true '
        f'{preview}'
    )


def audio_chain(cfg):
    src = cfg['audioInput']
    if src == 'test':
        head = 'audiotestsrc is-live=true wave=sine freq=440'
    elif src.startswith('alsa:'):
        head = f'alsasrc device={src[5:]}'
    elif src.startswith('pulse:'):
        head = f'pulsesrc device={src[6:]}'
    elif src in ('default', 'auto'):
        head = 'autoaudiosrc'
    else:
        head = f'filesrc location={src} ! decodebin'
    return (
        f'{head} ! audioconvert ! audioresample ! '
        f'audio/x-raw,rate=48000,channels={cfg["channels"]} ! '
        f'opusenc bitrate={cfg["audioBitrate"]} ! '
        f'tee name=atee allow-not-linked=true'
    )


def normalize_bundle_only(sdp_text):
    """
    Make RFC 8843 `a=bundle-only` m-lines palatable to webrtcbin.

    AWS's storage offer marks its video section:

        m=video 0 UDP/TLS/RTP/SAVPF 126
        a=bundle-only

    Port 0 there means "only usable inside the BUNDLE group", not "rejected".
    webrtcbin reads the 0 as a rejection and answers a=inactive, so no video
    ever reaches the archive -- which is the entire point of ingestion.

    Rewriting the port to 9 (discard) and dropping the bundle-only attribute is
    safe: we answer with max-bundle and rtcp-mux, so every stream shares the
    first m-line's transport regardless of what these ports say.
    """
    lines = sdp_text.split('\n')
    sections = []           # [start_index, is_zero_port, has_bundle_only]
    current = None
    for i, raw in enumerate(lines):
        line = raw.rstrip('\r')
        if line.startswith('m='):
            current = [i, line.split(' ')[1] == '0', False]
            sections.append(current)
        elif current is not None and line == 'a=bundle-only':
            current[2] = True

    drop = set()
    changed = 0
    for start, zero_port, bundle_only in sections:
        if not (zero_port and bundle_only):
            continue
        parts = lines[start].rstrip('\r').split(' ')
        parts[1] = '9'
        lines[start] = ' '.join(parts)
        changed += 1
        # Remove the now-misleading attribute from this section only.
        for j in range(start + 1, len(lines)):
            stripped = lines[j].rstrip('\r')
            if stripped.startswith('m='):
                break
            if stripped == 'a=bundle-only':
                drop.add(j)
                break

    if not changed:
        return sdp_text, 0
    kept = [line for i, line in enumerate(lines) if i not in drop]
    return '\n'.join(kept), changed


class Peer:
    """One webrtcbin, fed from the shared tees."""

    def __init__(self, server, peer_id, recv_audio):
        self.server = server
        self.id = peer_id
        self.recv_audio = recv_audio
        self.elements = []
        self.webrtc = None
        # Names of inbound pads already handled. Keyed per pad, not per peer:
        # every inbound pad must end up either played or drained, and a pad
        # left unhandled back-pressures nicesrc into killing the pipeline.
        self.handled_pads = set()

    def build(self):
        cfg = self.server.cfg
        pipe = self.server.pipe

        self.webrtc = Gst.ElementFactory.make('webrtcbin', f'peer_{self.id}')
        if not self.webrtc:
            raise RuntimeError('webrtcbin could not be created')
        self.webrtc.set_property('bundle-policy', 'max-bundle')
        self.webrtc.set_property('latency', 40)
        # NOTE: do not touch webrtcbin's 'ice-agent' property to switch off
        # libnice's UPnP probe. On GStreamer 1.26 reading it hands back a ref
        # PyGObject drops immediately, freeing the ICE object underneath
        # webrtcbin -- every later ICE call then fails its GST_IS_WEBRTC_ICE
        # assertion and the helper segfaults. The UPnP retry warnings are noise.
        if cfg.get('stunServer'):
            self.webrtc.set_property('stun-server', cfg['stunServer'])
        for turn in cfg.get('turnServers', []):
            self.webrtc.emit('add-turn-server', turn)

        pipe.add(self.webrtc)
        self.elements.append(self.webrtc)

        # Tees are linked in on_offer(), not here: webrtcbin assigns sink pads
        # to m-lines in request order, so we must know the offer's media order
        # before linking or our video lands on the audio m-line and goes
        # 'inactive' -- which makes AWS drop the storage session.

        self.webrtc.connect('on-ice-candidate', self._on_ice)
        self.webrtc.connect('pad-added', self._on_pad_added)
        self.webrtc.connect('notify::connection-state', self._on_conn_state)
        self.webrtc.connect('notify::ice-connection-state', self._on_ice_state)
        self.webrtc.sync_state_with_parent()

        # A peer joining between keyframes would see nothing until the next
        # one, so ask the encoder for an immediate IDR.
        venc = pipe.get_by_name('venc')
        if venc:
            venc.send_event(Gst.Event.new_custom(
                Gst.EventType.CUSTOM_UPSTREAM,
                Gst.Structure.new_empty('GstForceKeyUnit')))
        log('debug', 'peer built', id=self.id, recvAudio=self.recv_audio)

    # ---------------------------------------------------------- negotiation

    def _media_order(self, sdp):
        """[(kind, payload_type), ...] in the offer's own m-line order."""
        order = []
        for i in range(sdp.medias_len()):
            media = sdp.get_media(i)
            kind = media.get_media()
            pt = None
            if media.formats_len() > 0:
                try:
                    pt = int(media.get_format(0))
                except (TypeError, ValueError):
                    pt = None
            order.append((kind, pt))
        return order

    DEFAULT_PT = {'video': 96, 'audio': 111}

    def _payloader_desc(self, kind, pt):
        """Per-peer RTP packetiser, pinned to the payload type this peer offered."""
        # No trailing capsfilter: parse_bin_from_description cannot end on bare
        # caps, and the payloader's pt property already stamps the payload type
        # onto its src pad, which is what webrtcbin reads.
        # leaky=2 is leaky=downstream: once the branch holds more than
        # max-size-time the oldest buffer is dropped, so a peer that cannot keep
        # up falls behind by a bounded amount instead of an ever-growing one.
        # That bound is also pure added latency on every peer, hence 150ms
        # rather than the 300ms this used to carry.
        queue = f'queue leaky=2 max-size-time={SEND_QUEUE_NS}'
        if kind == 'video':
            return (
                f'{queue} ! '
                f'rtph264pay config-interval=-1 aggregate-mode=zero-latency pt={pt}'
            )
        return f'{queue} ! rtpopuspay pt={pt}'

    def _link_sources(self, order):
        """Attach each shared tee to the sink pad for its m-line index."""
        pipe = self.server.pipe
        for index, (kind, offered_pt) in enumerate(order):
            tee_name = {'video': 'vtee', 'audio': 'atee'}.get(kind)
            tee = pipe.get_by_name(tee_name) if tee_name else None
            if tee is None:
                log('debug', 'no source for m-line', id=self.id, index=index, kind=kind)
                continue
            pt = offered_pt if offered_pt is not None else self.DEFAULT_PT[kind]

            desc = self._payloader_desc(kind, pt)
            try:
                branch = Gst.parse_bin_from_description(desc, True)
            except GLib.Error as err:
                log('error', 'could not build payloader', id=self.id,
                    kind=kind, error=str(err))
                continue
            branch.set_name(f'{tee_name}_branch_{self.id}')
            pipe.add(branch)
            self.elements.append(branch)
            branch.sync_state_with_parent()

            if not tee.link(branch):
                log('error', 'tee -> payloader link failed', id=self.id, tee=tee_name)
                continue

            pad_name = f'sink_{index}'
            if hasattr(self.webrtc, 'request_pad_simple'):
                sink_pad = self.webrtc.request_pad_simple(pad_name)
            else:
                sink_pad = self.webrtc.get_request_pad(pad_name)
            if sink_pad is None:
                log('error', 'could not get webrtcbin pad', id=self.id, pad=pad_name)
                continue
            result = branch.get_static_pad('src').link(sink_pad)
            log('debug', 'linked source to m-line', id=self.id, kind=kind,
                pad=pad_name, payloadType=pt, result=result.value_nick)

    def on_offer(self, sdp_text):
        sdp_text, fixed = normalize_bundle_only(sdp_text)
        if fixed:
            log('info', 'normalized bundle-only m-line(s) for webrtcbin',
                id=self.id, count=fixed)
        res, sdp = GstSdp.SDPMessage.new_from_text(sdp_text)
        if res != GstSdp.SDPResult.OK:
            log('error', 'could not parse offer SDP', id=self.id)
            return
        order = self._media_order(sdp)
        log('info', 'offer media order', id=self.id,
            order=','.join(f'{k}:pt{p}' for k, p in order))
        self._link_sources(order)
        offer = GstWebRTC.WebRTCSessionDescription.new(
            GstWebRTC.WebRTCSDPType.OFFER, sdp)
        # Gst.Promise invokes the change func as (promise, user_data).
        promise = Gst.Promise.new_with_change_func(
            lambda _p, _u=None: self._create_answer(), None)
        self.webrtc.emit('set-remote-description', offer, promise)
        log('info', 'remote offer applied', id=self.id, bytes=len(sdp_text))

    def _create_answer(self):
        promise = Gst.Promise.new_with_change_func(self._on_answer, None)
        self.webrtc.emit('create-answer', None, promise)

    def _on_answer(self, promise, _=None):
        reply = promise.get_reply()
        answer = reply.get_value('answer')
        self.webrtc.emit('set-local-description', answer, Gst.Promise.new())
        text = answer.sdp.as_text()
        emit({'type': 'answer', 'id': self.id, 'sdp': text})
        log('info', 'answer sent', id=self.id, bytes=len(text))

    def add_ice(self, candidate, mline_index):
        self.webrtc.emit('add-ice-candidate', mline_index, candidate)

    # --------------------------------------------------------------- events

    def _on_ice(self, _elem, mline_index, candidate):
        emit({'type': 'ice', 'id': self.id,
              'candidate': candidate, 'sdpMLineIndex': mline_index})

    def _on_conn_state(self, elem, _param):
        emit({'type': 'state', 'id': self.id,
              'connection': elem.get_property('connection-state').value_nick})

    def _on_ice_state(self, elem, _param):
        emit({'type': 'state', 'id': self.id,
              'ice': elem.get_property('ice-connection-state').value_nick})

    def _probe_inbound(self, pad, kind):
        """Count buffers so we can distinguish a negotiated pad from a live one."""
        state = {'buffers': 0, 'bytes': 0, 'reported': False}

        def on_buffer(_pad, info):
            buf = info.get_buffer()
            if buf is not None:
                state['buffers'] += 1
                state['bytes'] += buf.get_size()
                if not state['reported']:
                    state['reported'] = True
                    log('info', f'FIRST inbound {kind} buffer received',
                        id=self.id, bytes=buf.get_size())
            return Gst.PadProbeReturn.OK

        pad.add_probe(Gst.PadProbeType.BUFFER, on_buffer)

        def tick():
            if state['buffers'] == 0:
                return True     # nothing arriving; stay quiet
            log('debug', f'inbound {kind} so far', id=self.id,
                buffers=state['buffers'], bytes=state['bytes'])
            return True

        GLib.timeout_add_seconds(5, tick)

    def _drain(self, pad, why):
        """Sink a pad we have no use for.

        webrtcbin still pushes data on an inbound pad nobody linked, and the
        not-linked flow return travels back up to nicesrc, which turns it into
        'Internal data stream error' and takes the whole pipeline down. AWS
        declares the storage session's audio sendrecv, so this pad always shows
        up even when we never asked for talk-back."""
        sink = Gst.ElementFactory.make('fakesink', None)
        if not sink:
            log('error', 'could not create fakesink for unused pad', id=self.id)
            return
        sink.set_property('sync', False)
        sink.set_property('async', False)
        self.server.pipe.add(sink)
        self.elements.append(sink)
        sink.sync_state_with_parent()
        result = pad.link(sink.get_static_pad('sink'))
        log('info', 'draining unused inbound pad', id=self.id, why=why,
            result=result.value_nick)

    def _on_pad_added(self, _elem, pad):
        """Inbound media from this peer (talk-back audio, normally)."""
        if pad.direction != Gst.PadDirection.SRC:
            return
        caps_now = pad.get_current_caps()
        log('info', 'INBOUND PAD appeared', id=self.id,
            caps=(caps_now.to_string()[:70] if caps_now else '(none yet)'))
        self._probe_inbound(pad, 'media')
        if not self.recv_audio:
            log('info', 'recvAudio disabled for this peer — not attaching a sink',
                id=self.id)
            self._drain(pad, 'recvAudio disabled')
            return
        if caps_now is not None:
            self._attach_inbound(pad, caps_now)
            return
        # webrtcbin usually adds the pad before its caps are negotiated, so the
        # pad reads as having no caps at all for a moment. Classifying it now
        # would file a perfectly good Opus pad under 'non-audio' and drain it --
        # which is exactly how talk-back goes silent. Wait for the CAPS event
        # instead, and attach from the main loop rather than the streaming
        # thread, since linking from inside a probe can deadlock.
        log('debug', 'inbound pad has no caps yet — waiting for the CAPS event',
            id=self.id)

        def on_event(probed_pad, info):
            event = info.get_event()
            if event is None or event.type != Gst.EventType.CAPS:
                return Gst.PadProbeReturn.OK
            GLib.idle_add(self._attach_inbound, probed_pad, event.parse_caps())
            return Gst.PadProbeReturn.REMOVE

        pad.add_probe(Gst.PadProbeType.EVENT_DOWNSTREAM, on_event)

    def _attach_inbound(self, pad, caps):
        """Play this pad if it carries Opus, drain it otherwise. Idempotent:
        reached both directly and from the deferred CAPS probe."""
        key = pad.get_name()
        if key in self.handled_pads or pad.is_linked():
            return False
        self.handled_pads.add(key)
        name = caps.to_string() if caps else ''
        if 'OPUS' not in name.upper() and 'audio' not in name:
            log('debug', 'ignoring inbound non-audio pad', id=self.id,
                caps=name[:70])
            self._drain(pad, 'non-audio pad')
            return False
        sink = self.server.cfg.get('audioOutPipeline') or 'autoaudiosink sync=false'
        # leaky=downstream caps how far behind talk-back may fall. The sink runs
        # sync=false, so nothing downstream ever drops a late buffer: without
        # this a burst of packets is played out in full and the delay it added
        # stays for the rest of the session. plc conceals what the queue drops.
        queue_ns = int(self.server.cfg.get('talkbackQueueMs') or 60) * 1_000_000
        chain = (f'rtpopusdepay ! opusdec plc=true ! audioconvert ! audioresample ! '
                 f'queue leaky=downstream max-size-time={queue_ns} '
                 f'max-size-buffers=0 max-size-bytes=0')
        desc = f'{chain} ! {sink}'
        try:
            bin_ = Gst.parse_bin_from_description(desc, True)
        except GLib.Error as err:
            # A box without the PulseAudio plugin cannot build the configured
            # sink. Falling back beats losing talk-back altogether.
            log('warn', 'audio sink could not be built — falling back',
                id=self.id, sink=sink, error=str(err))
            sink = 'autoaudiosink sync=false'
            try:
                bin_ = Gst.parse_bin_from_description(f'{chain} ! {sink}', True)
            except GLib.Error as err2:
                log('error', 'failed to build audio sink', id=self.id, error=str(err2))
                return False
        self.server.pipe.add(bin_)
        self.elements.append(bin_)
        bin_.sync_state_with_parent()
        result = pad.link(bin_.get_static_pad('sink'))
        log('info', 'playing inbound audio', id=self.id, sink=sink,
            caps=name[:70], link=result.value_nick)
        return False    # one-shot when called via GLib.idle_add

    def destroy(self):
        for el in self.elements:
            el.set_state(Gst.State.NULL)
            self.server.pipe.remove(el)
        self.elements = []
        log('info', 'peer torn down', id=self.id)


class Server:
    def __init__(self, cfg):
        self.cfg = cfg
        self.loop = GLib.MainLoop()
        self.peers = {}
        self.pipe = None

    def build(self):
        desc = f'{video_chain(self.cfg)} {audio_chain(self.cfg)}'
        log('debug', 'gstreamer pipeline', desc=desc)
        self.pipe = Gst.parse_launch(desc)

        bus = self.pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message::error', self._on_error)
        bus.connect('message::warning', self._on_warning)
        bus.connect('message::eos', lambda *_: log('warn', 'pipeline EOS'))

        self._attach_preview()

        self.pipe.set_state(Gst.State.PLAYING)
        log('info', 'media pipeline PLAYING', video=self.cfg['videoInput'],
            audio=self.cfg['audioInput'])
        emit({'type': 'ready'})

    def _attach_preview(self):
        """Forward each JPEG the preview appsink produces to Node, base64'd."""
        sink = self.pipe.get_by_name('preview')
        if sink is None:
            return
        sink.connect('new-sample', self._on_preview_sample)
        log('debug', 'preview appsink attached', fps=self.cfg.get('previewFps'))

    def _on_preview_sample(self, sink):
        sample = sink.emit('pull-sample')
        if sample is None:
            return Gst.FlowReturn.OK
        buf = sample.get_buffer()
        ok, info = buf.map(Gst.MapFlags.READ)
        if not ok:
            return Gst.FlowReturn.OK
        try:
            emit({'type': 'preview',
                  'jpeg': base64.b64encode(info.data).decode('ascii')})
        finally:
            buf.unmap(info)
        return Gst.FlowReturn.OK

    def add_peer(self, peer_id, recv_audio):
        if peer_id in self.peers:
            self.remove_peer(peer_id)
        peer = Peer(self, peer_id, recv_audio)
        try:
            peer.build()
        except Exception as err:                      # noqa: BLE001
            log('error', 'failed to build peer', id=peer_id, error=str(err))
            return
        self.peers[peer_id] = peer
        log('info', 'peer added', id=peer_id, total=len(self.peers))

    def dispatch_offer(self, peer_id, sdp):
        peer = self.peers.get(peer_id)
        if peer is None:
            log('warn', 'offer for unknown peer', id=peer_id)
            return
        peer.on_offer(sdp)

    def dispatch_ice(self, peer_id, candidate, mline_index):
        peer = self.peers.get(peer_id)
        if peer is None:
            log('debug', 'ICE for unknown peer, dropping', id=peer_id)
            return
        peer.add_ice(candidate, mline_index)

    def remove_peer(self, peer_id):
        peer = self.peers.pop(peer_id, None)
        if peer:
            peer.destroy()
            log('info', 'peer removed', id=peer_id, remaining=len(self.peers))

    def _on_error(self, _bus, message):
        err, debug = message.parse_error()
        src = message.src.get_name() if message.src else '?'
        log('error', 'gstreamer error', element=src, error=err.message,
            debug=(debug or '')[:200])
        # An error on a source element is unrecoverable; peer errors are not.
        if src in ('venc', 'aenc') or 'src' in src:
            emit({'type': 'fatal', 'msg': f'{src}: {err.message}'})

    def _on_warning(self, _bus, message):
        err, _ = message.parse_warning()
        log('warn', 'gstreamer warning', error=err.message)

    def stop(self):
        for peer_id in list(self.peers):
            self.remove_peer(peer_id)
        if self.pipe:
            self.pipe.set_state(Gst.State.NULL)
        self.loop.quit()


def reader(server):
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            log('warn', 'bad JSON from node', raw=line[:80])
            continue

        kind = msg.get('type')
        pid = msg.get('id')
        # Peer lookup must happen on the main loop, not here: add_peer is
        # itself deferred, so looking up in this thread races ahead of it.
        # idle callbacks run in submission order, so add_peer always wins.
        if kind == 'add_peer':
            GLib.idle_add(server.add_peer, pid, msg.get('recvAudio', True))
        elif kind == 'offer':
            GLib.idle_add(server.dispatch_offer, pid, msg['sdp'])
        elif kind == 'ice':
            GLib.idle_add(server.dispatch_ice, pid, msg['candidate'],
                          msg.get('sdpMLineIndex', 0))
        elif kind == 'remove_peer':
            GLib.idle_add(server.remove_peer, pid)
        elif kind == 'stop':
            break
    GLib.idle_add(server.stop)


def main():
    cfg = json.loads(sys.argv[1])
    server = Server(cfg)
    try:
        server.build()
    except GLib.Error as err:
        emit({'type': 'fatal', 'msg': str(err)})
        sys.exit(1)
    threading.Thread(target=reader, args=(server,), daemon=True).start()
    try:
        server.loop.run()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == '__main__':
    main()
