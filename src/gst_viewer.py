#!/usr/bin/env python3
"""
Receive-only WebRTC peer for the KVS storage session.

With media ingestion enabled the master does not serve viewers directly: it
streams into the Kinesis Video Stream, and each viewer calls
JoinStorageSessionAsViewer so AWS fans the media out. AWS then sends *us* the
offer, exactly as it does to the master.

Video is H.264, which @roamhq/wrtc cannot decode -- hence GStreamer.

Protocol: newline-delimited JSON on stdin/stdout (see gst_peer.py).
"""
import json
import sys
import threading

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstWebRTC', '1.0')
gi.require_version('GstSdp', '1.0')
from gi.repository import Gst, GstWebRTC, GstSdp, GLib  # noqa: E402

Gst.init(None)

_out_lock = threading.Lock()


def emit(obj):
    with _out_lock:
        sys.stdout.write(json.dumps(obj) + '\n')
        sys.stdout.flush()


def log(level, msg, **fields):
    emit({'type': 'log', 'level': level, 'msg': msg, 'fields': fields})


def normalize_bundle_only(sdp_text):
    """See gst_peer.py: webrtcbin treats a bundle-only port 0 as rejected."""
    lines = sdp_text.split('\n')
    sections, current = [], None
    for i, raw in enumerate(lines):
        line = raw.rstrip('\r')
        if line.startswith('m='):
            current = [i, line.split(' ')[1] == '0', False]
            sections.append(current)
        elif current is not None and line == 'a=bundle-only':
            current[2] = True

    drop, changed = set(), 0
    for start, zero_port, bundle_only in sections:
        if not (zero_port and bundle_only):
            continue
        parts = lines[start].rstrip('\r').split(' ')
        parts[1] = '9'
        lines[start] = ' '.join(parts)
        changed += 1
        for j in range(start + 1, len(lines)):
            stripped = lines[j].rstrip('\r')
            if stripped.startswith('m='):
                break
            if stripped == 'a=bundle-only':
                drop.add(j)
                break
    if not changed:
        return sdp_text, 0
    return '\n'.join(l for i, l in enumerate(lines) if i not in drop), changed


class Viewer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.loop = GLib.MainLoop()
        self.pipe = Gst.Pipeline.new('viewer')
        self.webrtc = None
        self.seen = {'video': 0, 'audio': 0}

    def build(self):
        self.webrtc = Gst.ElementFactory.make('webrtcbin', 'recv')
        self.webrtc.set_property('bundle-policy', 'max-bundle')
        self.webrtc.set_property('latency', self.cfg.get('latencyMs', 100))
        if self.cfg.get('stunServer'):
            self.webrtc.set_property('stun-server', self.cfg['stunServer'])
        for turn in self.cfg.get('turnServers', []):
            self.webrtc.emit('add-turn-server', turn)

        self.pipe.add(self.webrtc)
        self.webrtc.connect('on-ice-candidate', self._on_ice)
        self.webrtc.connect('pad-added', self._on_pad)
        self.webrtc.connect('notify::connection-state', self._on_conn)
        self.webrtc.connect('notify::ice-connection-state', self._on_ice_state)

        bus = self.pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message::error', self._on_error)

        self.pipe.set_state(Gst.State.PLAYING)
        log('info', 'viewer pipeline PLAYING')
        emit({'type': 'ready'})

    def _report_latency(self):
        """Playback-side contribution to the delay. Logged once media is
        actually flowing, since an empty pipeline has nothing to report."""
        query = Gst.Query.new_latency()
        if not self.pipe.query(query):
            log('debug', 'pipeline did not answer the latency query')
            return False
        live, min_ns, max_ns = query.parse_latency()
        log('info', 'playback-side pipeline latency', live=live,
            jitterBufferMs=self.cfg.get('latencyMs', 100),
            queueCapMs=self.cfg.get('queueMs', 200),
            minMs=round(min_ns / 1e6, 1),
            maxMs=(round(max_ns / 1e6, 1) if max_ns != Gst.CLOCK_TIME_NONE else None))
        return False

    # ---------------------------------------------------------- negotiation

    def _audio_source_desc(self, pt):
        """Talk-back chain. AWS offers the viewer's audio m-line as sendrecv, so
        a viewer may send audio back; it is relayed to the master and to the
        other viewers, and ingested into the stream."""
        src = self.cfg.get('audioInput', 'none')
        if src == 'test':
            head = 'audiotestsrc is-live=true wave=sine freq=660'
        elif src.startswith('alsa:'):
            head = f'alsasrc device={src[5:]}'
        elif src.startswith('pulse:'):
            head = f'pulsesrc device={src[6:]}'
        elif src in ('default', 'auto'):
            head = 'autoaudiosrc'
        else:
            head = f'filesrc location={src} ! decodebin'
        return (f'{head} ! audioconvert ! audioresample ! '
                f'audio/x-raw,rate=48000,channels=1 ! '
                f'opusenc bitrate={self.cfg.get("audioBitrate", 64000)} ! '
                f'rtpopuspay pt={pt}')

    def _link_talkback(self, sdp):
        """Attach the mic to the offer's audio m-line, before we answer."""
        if self.cfg.get('audioInput', 'none') in ('none', None, ''):
            log('info', 'no AUDIO_INPUT set — viewer will answer recvonly (no talk-back)')
            return
        for i in range(sdp.medias_len()):
            media = sdp.get_media(i)
            if media.get_media() != 'audio':
                continue
            try:
                pt = int(media.get_format(0))
            except (TypeError, ValueError):
                pt = 111
            desc = self._audio_source_desc(pt)
            try:
                branch = Gst.parse_bin_from_description(desc, True)
            except GLib.Error as err:
                log('error', 'could not build talk-back source', error=str(err))
                return
            self.pipe.add(branch)
            branch.sync_state_with_parent()
            pad_name = f'sink_{i}'
            if hasattr(self.webrtc, 'request_pad_simple'):
                sink_pad = self.webrtc.request_pad_simple(pad_name)
            else:
                sink_pad = self.webrtc.get_request_pad(pad_name)
            if sink_pad is None:
                log('error', 'could not get webrtcbin pad for talk-back', pad=pad_name)
                return
            result = branch.get_static_pad('src').link(sink_pad)
            log('info', 'talk-back source linked', pad=pad_name,
                payloadType=pt, source=self.cfg.get('audioInput'),
                link=result.value_nick)
            return
        log('warn', 'offer had no audio m-line — cannot send talk-back')

    def on_offer(self, sdp_text):
        sdp_text, fixed = normalize_bundle_only(sdp_text)
        if fixed:
            log('info', 'normalized bundle-only m-line(s)', count=fixed)
        res, sdp = GstSdp.SDPMessage.new_from_text(sdp_text)
        if res != GstSdp.SDPResult.OK:
            log('error', 'could not parse offer SDP')
            return
        # Must happen before set-remote-description so the audio transceiver is
        # sendrecv rather than recvonly.
        self._link_talkback(sdp)
        offer = GstWebRTC.WebRTCSessionDescription.new(
            GstWebRTC.WebRTCSDPType.OFFER, sdp)
        def applied(p, _u=None):
            if p.wait() != Gst.PromiseResult.REPLIED:
                log('error', 'set-remote-description failed')
                return
            reply = p.get_reply()
            if reply is not None and reply.has_field('error'):
                log('error', 'remote description rejected',
                    reply=reply.to_string()[:300])
                return
            self._create_answer()

        promise = Gst.Promise.new_with_change_func(applied, None)
        self.webrtc.emit('set-remote-description', offer, promise)
        log('info', 'remote offer applied', bytes=len(sdp_text))

    def _create_answer(self):
        promise = Gst.Promise.new_with_change_func(self._on_answer, None)
        self.webrtc.emit('create-answer', None, promise)

    def _on_answer(self, promise, _=None):
        result = promise.wait()
        if result != Gst.PromiseResult.REPLIED:
            log('error', 'create-answer did not reply', result=str(result))
            return
        reply = promise.get_reply()
        if reply is None:
            log('error', 'create-answer returned no reply structure')
            return
        answer = reply.get_value('answer')
        if answer is None:
            # Surface whatever webrtcbin did put in the reply; an 'error' field
            # here is the actual reason negotiation failed.
            log('error', 'create-answer produced no answer',
                reply=reply.to_string()[:300])
            return
        self.webrtc.emit('set-local-description', answer, Gst.Promise.new())
        emit({'type': 'answer', 'sdp': answer.sdp.as_text()})

    def add_ice(self, candidate, mline_index):
        self.webrtc.emit('add-ice-candidate', mline_index, candidate)

    # -------------------------------------------------------------- sinks

    def _on_pad(self, _elem, pad):
        if pad.direction != Gst.PadDirection.SRC:
            return
        caps = pad.get_current_caps()
        text = caps.to_string() if caps else ''
        upper = text.upper()
        if 'H264' in upper:
            kind, desc = 'video', self._video_sink()
        elif 'OPUS' in upper:
            kind, desc = 'audio', self._audio_sink()
        else:
            log('debug', 'ignoring unknown inbound pad', caps=text[:70])
            return

        self.seen[kind] += 1
        try:
            bin_ = Gst.parse_bin_from_description(desc, True)
        except GLib.Error as err:
            log('error', f'failed to build {kind} sink', error=str(err), desc=desc)
            return
        self.pipe.add(bin_)
        bin_.sync_state_with_parent()
        result = pad.link(bin_.get_static_pad('sink'))
        log('info', f'receiving {kind}', sink=desc[:70], link=result.value_nick)
        emit({'type': 'media', 'kind': kind})
        GLib.timeout_add_seconds(3, self._report_latency)

    def _live_queue(self):
        """
        Decouple decoding from webrtcbin's streaming thread, with a hard cap.

        Two things go wrong without this. Decoding inline means avdec_h264 runs
        on the thread feeding the jitter buffer, so a slow frame back-pressures
        reception itself. And an unbounded queue turns every network hiccup into
        permanent latency: the backlog is played out, never skipped.

        leaky=downstream drops the oldest buffer once the branch holds more than
        `queueMs`, so lateness stays bounded no matter how long the run is.
        """
        ns = int(self.cfg.get('queueMs', 200)) * 1_000_000
        return (f'queue leaky=downstream max-size-time={ns} '
                f'max-size-buffers=0 max-size-bytes=0')

    def _video_sink(self):
        mode = self.cfg.get('videoOut', 'play')
        head = f'rtph264depay ! h264parse ! {self._live_queue()}'
        if mode == 'file':
            path = self.cfg['videoFile']
            # Fragmented MP4: writes as it goes, so the recording survives an
            # abrupt exit. Plain mp4mux only writes its index at EOS, which
            # leaves a 0-byte file if the process is killed.
            return (f'{head} ! mp4mux faststart=false fragment-duration=1000 '
                    f'! filesink location={path} sync=false')
        if mode == 'none':
            return f'{head} ! fakesink sync=false'
        # sync=true is load-bearing for live playback. It puts both sinks on the
        # pipeline clock (which is what lip-syncs them from the RTCP sender
        # reports) and, just as importantly, arms QoS: a frame that arrives past
        # its deadline is dropped instead of shown late. With sync=false every
        # sink renders on arrival, so lag accumulates and never recovers.
        return f'{head} ! avdec_h264 ! videoconvert ! autovideosink sync=true'

    def _audio_sink(self):
        mode = self.cfg.get('audioOut', 'play')
        head = ('rtpopusdepay ! opusdec ! audioconvert ! audioresample ! '
                f'{self._live_queue()}')
        if mode == 'file':
            path = self.cfg['audioFile']
            return f'{head} ! wavenc ! filesink location={path}'
        if mode == 'none':
            return f'{head} ! fakesink sync=false'
        device = self.cfg.get('audioDevice')
        if device:
            return f'{head} ! pulsesink device={device} sync=true'
        return f'{head} ! autoaudiosink sync=true'

    # ------------------------------------------------------------- events

    def _on_ice(self, _elem, mline_index, candidate):
        emit({'type': 'ice', 'candidate': candidate, 'sdpMLineIndex': mline_index})

    def _on_conn(self, elem, _p):
        emit({'type': 'state',
              'connection': elem.get_property('connection-state').value_nick})

    def _on_ice_state(self, elem, _p):
        emit({'type': 'state',
              'ice': elem.get_property('ice-connection-state').value_nick})

    def _on_error(self, _bus, message):
        err, debug = message.parse_error()
        log('error', 'gstreamer error', error=err.message, debug=(debug or '')[:200])

    def stop(self):
        # EOS first so mp4mux/wavenc finalise their headers, else the recording
        # is unplayable.
        self.pipe.send_event(Gst.Event.new_eos())
        GLib.timeout_add(700, self._finalise)

    def _finalise(self):
        self.pipe.set_state(Gst.State.NULL)
        self.loop.quit()
        return False


def reader(viewer):
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get('type') == 'offer':
            GLib.idle_add(viewer.on_offer, msg['sdp'])
        elif msg.get('type') == 'ice':
            GLib.idle_add(viewer.add_ice, msg['candidate'], msg.get('sdpMLineIndex', 0))
        elif msg.get('type') == 'stop':
            break
    GLib.idle_add(viewer.stop)


def main():
    cfg = json.loads(sys.argv[1])
    viewer = Viewer(cfg)
    viewer.build()
    threading.Thread(target=reader, args=(viewer,), daemon=True).start()
    try:
        viewer.loop.run()
    finally:
        viewer.pipe.set_state(Gst.State.NULL)


if __name__ == '__main__':
    main()
