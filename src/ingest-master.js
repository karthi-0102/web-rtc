'use strict';

/**
 * KVS WebRTC MASTER with media ingestion + multiviewer.
 *
 *   sends   : H.264 video + Opus audio, simultaneously to
 *               - the KVS storage session (archived into a Kinesis Video Stream)
 *               - every live viewer that connects (peer-to-peer)
 *   receives: audio from live viewers (talk-back)
 *
 * Storage and live are the same media, encoded once and fanned out by a tee in
 * the GStreamer helper. AWS's storage session appears on the signaling channel
 * as just another peer sending an offer, so it flows through the same path as a
 * human viewer — including the audio it sends back, which carries the talk-back
 * of every viewer AWS is fanning out to.
 *
 * H.264 is mandatory for ingestion, which is why this path uses GStreamer
 * rather than the @roamhq/wrtc used by master.js.
 */

const fs = require('fs');
const path = require('path');

const { config, resolveCredentials, logConfig } = require('./config');
const {
  describeChannel, createSignalingClient, Role,
} = require('./kvs');
const { GstMedia } = require('./gst');
const { createLogger, logSdp, summarizeCandidate } = require('./log');
const { createMonitor, instrumentSignaling } = require('./monitor');
const {
  KinesisVideoWebRTCStorageClient,
  JoinStorageSessionCommand,
} = require('@aws-sdk/client-kinesis-video-webrtc-storage');

const log = createLogger('ingest');

// AWS's storage peer identifies itself with this clientId on the signaling
// channel. Anything else is a human viewer.
const STORAGE_CLIENT_ID = 'MEDIA_STORAGE';

const peers = new Map();        // signaling client id -> entry
// AWS can re-offer the storage session before the helper has answered the
// previous offer. Both webrtcbins then emit an answer, and keying only by
// client id would let the dead peer's answer be sent as the live one's --
// AWS would latch onto its ICE credentials while every candidate we trickle
// comes from the new peer, and the connection never gets past checking. Each
// generation gets its own helper-side id so stale traffic finds no entry.
const peersByMediaId = new Map(); // helper peer id -> entry
let peerGeneration = 0;

function isStoragePeer(clientId) {
  return !clientId || String(clientId).toUpperCase().includes('STORAGE');
}

/**
 * Where inbound talk-back audio goes. AWS relays each viewer's audio to the
 * master, so this is a real signal, not a formality.
 */
function buildAudioSink() {
  if (config.output.audio === 'none') return 'fakesink';
  if (config.output.audio === 'file') {
    fs.mkdirSync(config.output.dir, { recursive: true });
    const file = path.join(config.output.dir, `talkback-${process.pid}.wav`);
    log.info('recording inbound talk-back audio', { file });
    return `wavenc ! filesink location=${file}`;
  }
  // buffer-time is the sink's ring buffer and the floor on how late talk-back
  // can be heard; the 200ms pulsesink default dominates every other local term
  // in the path. latency-time is the write granularity underneath it.
  // The helper falls back to autoaudiosink if pulsesink cannot be built.
  const bufferUs = Math.max(10000, Math.round(config.talkback.sinkMs * 1000));
  const device = config.output.audioDevice ? ` device=${config.output.audioDevice}` : '';
  return `pulsesink${device} sync=false buffer-time=${bufferUs} latency-time=10000`;
}

async function main() {
  log.info('KVS WebRTC ingestion master starting');
  logConfig('MASTER (ingest + multiviewer)');

  // Local dashboard: published video thumbnail + every signaling message.
  // Disable with MONITOR=false.
  const monitor = createMonitor({ role: 'ingest-master', config });
  if (monitor.url) log.info(`open ${monitor.url} to watch what this master publishes`);

  const credentials = await resolveCredentials();

  // INGEST_SKIP_STORAGE runs this master as a plain H.264 multiviewer, peering
  // with viewers directly and archiving nothing -- useful when the channel has
  // no MediaStorageConfiguration, and as a control when testing talk-back.
  const skipStorage = /^(1|true|yes|on)$/i.test(process.env.INGEST_SKIP_STORAGE || '');

  // WEBRTC is only returned when the channel has MediaStorageConfiguration
  // ENABLED; kvs.js throws with the fix-it command if it is missing.
  const { channelARN, endpoints, iceServers } = await describeChannel({
    region: config.region,
    credentials,
    channelName: config.channelName,
    role: Role.MASTER,
    protocols: skipStorage ? ['WSS', 'HTTPS'] : ['WSS', 'HTTPS', 'WEBRTC'],
  });

  // ---------------------------------------------------------------- media
  const previewFps = monitor.enabled
    ? Math.max(1, Number(process.env.MONITOR_PREVIEW_FPS) || 4)
    : 0;
  const media = new GstMedia({
    media: { video: config.video, audio: config.audio },
    iceServers,
    logger: createLogger('gst'),
    audioOutPipeline: buildAudioSink(),
    talkbackQueueMs: config.talkback.queueMs,
    clockOverlay: config.clockOverlay,
    previewFps,
  });
  // The GStreamer helper renders its own thumbnails (the media never enters
  // Node here), so feed them straight to the dashboard.
  media.on('preview', (jpeg) => monitor.frame(jpeg));
  monitor.setMedia({
    video: {
      input: config.video.input,
      size: `${config.video.width}x${config.video.height}`,
      fps: config.video.fps,
      codec: `H.264 @ ${config.video.bitrateKbps} kbps`,
    },
    audio: {
      input: config.audio.input,
      sampleRate: config.audio.sampleRate,
      channels: config.audio.channels,
      codec: `Opus @ ${Math.round(config.audio.bitrate / 1000)} kbps`,
    },
    archiving: !skipStorage,
  });
  media.on('fatal', (msg) => {
    log.error('media pipeline died — exiting', { error: msg });
    shutdown(1);
  });
  await media.whenReady();
  log.step('media pipeline ready (H.264 + Opus, shared across all peers)');

  // ------------------------------------------------------------ signaling
  const signalingClient = instrumentSignaling(createSignalingClient({
    region: config.region,
    credentials,
    channelARN,
    endpoint: endpoints.WSS,
    role: Role.MASTER,
  }), monitor);

  // Helper answers/candidates are addressed by peer id; forward them back over
  // signaling to the right remote client.
  media.on('answer', (mediaId, sdp) => {
    const entry = peersByMediaId.get(mediaId);
    if (!entry) return;
    logSdp(entry.log, 'answer', { type: 'answer', sdp });
    signalingClient.sendSdpAnswer({ type: 'answer', sdp }, entry.remoteClientId);
    entry.log.step('SDP ANSWER sent');
  });

  media.on('ice', (mediaId, cand) => {
    const entry = peersByMediaId.get(mediaId);
    if (!entry) return;
    entry.log.debug('local ICE candidate -> peer', summarizeCandidate(cand.candidate));
    signalingClient.sendIceCandidate(
      { candidate: cand.candidate, sdpMLineIndex: cand.sdpMLineIndex, sdpMid: null },
      entry.remoteClientId,
    );
  });

  media.on('state', (mediaId, state) => {
    const entry = peersByMediaId.get(mediaId);
    if (!entry) return;
    if (state.ice) entry.log.info(`ICE connection -> ${state.ice}`);
    monitor.peer(entry.id, {
      kind: entry.isStorage ? 'aws-storage' : 'viewer',
      ice: state.ice || undefined,
      state: state.connection || undefined,
    });
    if (!state.connection) return;
    entry.log.step(`connection state -> ${state.connection}`);
    if (state.connection === 'connected') {
      entry.connectedAt = Date.now();
      entry.log.info(entry.isStorage
        ? 'ARCHIVING — media now flowing into the Kinesis Video Stream'
        : 'LIVE — media now flowing to this viewer');
      report();
    }
    if (['failed', 'closed', 'disconnected'].includes(state.connection)) closePeer(entry.id);
  });

  signalingClient.on('open', async () => {
    log.step('signaling websocket OPEN');
    if (skipStorage) {
      log.info('INGEST_SKIP_STORAGE set — not archiving; serving viewers directly');
      return;
    }
    await joinStorageSession({ credentials, channelARN, endpoint: endpoints.WEBRTC });
  });

  signalingClient.on('sdpOffer', (offer, remoteClientId) => {
    const storage = isStoragePeer(remoteClientId);
    const id = remoteClientId || STORAGE_CLIENT_ID;
    const peerLog = log.child(storage ? 'storage' : id);

    peerLog.step(`SDP OFFER received from ${storage ? 'AWS storage session' : 'viewer'}`, {
      clientId: remoteClientId || '(none)',
    });
    logSdp(peerLog, 'offer', offer);

    if (peers.has(id)) {
      peerLog.info('peer re-offered — replacing previous session');
      closePeer(id);
    }
    const mediaId = `${id}_g${++peerGeneration}`;
    const entry = {
      id,
      mediaId,
      isStorage: storage,
      remoteClientId,
      log: peerLog,
      connectedAt: null,
    };
    peers.set(id, entry);
    peersByMediaId.set(mediaId, entry);
    monitor.peer(id, {
      kind: storage ? 'aws-storage' : 'viewer',
      state: 'negotiating',
      mediaId,
    });

    // Talk-back is on for every peer, storage included. AWS declares the
    // storage session's audio sendrecv and relays each viewer's microphone
    // through it, so draining that pad by default threw away the only channel a
    // viewer has back to the master. INGEST_RECV_STORAGE_AUDIO=false restores
    // the old drain-it behaviour for a master that genuinely only publishes.
    const recvAudio = storage
      ? !/^(0|false|no|off)$/i.test(process.env.INGEST_RECV_STORAGE_AUDIO || '')
      : true;
    media.addPeer(mediaId, { recvAudio });
    media.setRemoteOffer(mediaId, offer.sdp);
    report();
  });

  signalingClient.on('iceCandidate', (candidate, remoteClientId) => {
    const id = remoteClientId || STORAGE_CLIENT_ID;
    const entry = peers.get(id);
    if (!entry) {
      log.warn('ICE candidate for unknown peer — dropping', { clientId: id });
      return;
    }
    entry.log.debug('remote ICE candidate', summarizeCandidate(candidate));
    media.addIceCandidate(entry.mediaId, candidate.candidate, candidate.sdpMLineIndex);
  });

  signalingClient.on('close', () => log.warn('signaling websocket CLOSED'));
  signalingClient.on('error', (err) =>
    log.error('signaling error', { error: err.message || String(err) }));

  log.step('opening signaling websocket (SigV4-signed WSS)');
  signalingClient.open();

  /** Ask AWS to join as a peer and archive whatever we send it. */
  async function joinStorageSession({ credentials: creds, channelARN: arn, endpoint }) {
    const client = new KinesisVideoWebRTCStorageClient({
      region: config.region,
      credentials: creds,
      endpoint,
    });
    log.step('JoinStorageSession — asking AWS to connect as the archiving peer', {
      endpoint, stream: config.storageStreamName || '(from MediaStorageConfiguration)',
    });
    try {
      const started = Date.now();
      await client.send(new JoinStorageSessionCommand({ channelArn: arn }));
      log.info('JoinStorageSession accepted — expect an SDP offer from AWS shortly',
        { ms: Date.now() - started });
    } catch (err) {
      log.error('JoinStorageSession failed — archiving will not happen', {
        name: err.name, error: err.message,
      });
      log.warn('live viewers will still work; fix storage config and restart to archive');
    }
  }

  function report() {
    const live = [...peers.values()].filter((p) => !p.isStorage).length;
    const archiving = [...peers.values()].some((p) => p.isStorage);
    log.info('session summary', {
      liveViewers: live,
      archiving,
      totalPeers: peers.size,
    });
    monitor.setMedia({ liveViewers: live, archiving });
  }

  function shutdown(code = 0) {
    log.step('shutting down', { peers: peers.size });
    for (const id of [...peers.keys()]) closePeer(id);
    media.close();
    monitor.stop();
    try { signalingClient.close(); } catch { /* already closed */ }
    setTimeout(() => process.exit(code), 800).unref();
  }

  function closePeer(id) {
    const entry = peers.get(id);
    if (!entry) return;
    peers.delete(id);
    peersByMediaId.delete(entry.mediaId);
    const heldMs = entry.connectedAt ? Date.now() - entry.connectedAt : 0;
    entry.log.info('closing peer', { heldSeconds: Math.round(heldMs / 1000) });
    monitor.removePeer(id);
    media.removePeer(entry.mediaId);
  }

  process.on('SIGINT', () => shutdown(0));
  process.on('SIGTERM', () => shutdown(0));
  process.on('unhandledRejection', (err) =>
    log.error('unhandled rejection', { error: (err && err.message) || String(err) }));
}

main().catch((err) => {
  log.error('fatal', { error: err.message });
  console.error(err);
  process.exit(1);
});
