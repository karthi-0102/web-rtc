'use strict';

/**
 * KVS WebRTC MASTER — the access panel.
 *
 *   sends  : video + audio (camera/mic, or synthetic test sources)
 *   receives: audio from every viewer that connects (played or recorded)
 *
 * A master answers offers, so it can serve several viewers at once; each one
 * gets its own RTCPeerConnection fed from the same pair of ffmpeg processes.
 */

const { config, resolveCredentials, logConfig } = require('./config');
const {
  describeChannel, createSignalingClient, createCandidateQueue, attachConnectionLogging, Role,
} = require('./kvs');
const { VideoSource, AudioSource, AudioOutput, wrtc } = require('./media');
const { createLogger, logSdp, summarizeCandidate } = require('./log');
const { startStatsReporter } = require('./stats');
const { createMonitor, instrumentSignaling } = require('./monitor');

const { RTCPeerConnection, RTCSessionDescription, RTCIceCandidate } = wrtc;

const log = createLogger('master');
const peers = new Map(); // remoteClientId -> { peerConnection, audioOutput, stopStats, log }
let activeMonitor = null; // set in main(); closePeer() lives outside its scope

async function main() {
  log.info('KVS WebRTC master starting');
  logConfig('MASTER');

  // The monitor window: a local web page showing the published video and every
  // signaling message. Disable with MONITOR=false.
  const monitor = createMonitor({ role: 'master', config });
  activeMonitor = monitor;
  if (monitor.url) log.info(`open ${monitor.url} to watch what this master publishes`);

  const credentials = await resolveCredentials();

  const { channelARN, endpoints, iceServers } = await describeChannel({
    region: config.region,
    credentials,
    channelName: config.channelName,
    role: Role.MASTER,
  });

  const videoSource = new VideoSource({ ...config.video, debug: config.debug }).start();
  const audioSource = new AudioSource({ ...config.audio, debug: config.debug }).start();

  monitor.attachVideoSource(videoSource, config.video);
  monitor.setMedia({
    video: { input: config.video.input, size: `${config.video.width}x${config.video.height}`, fps: config.video.fps, codec: 'VP8 (wrtc)' },
    audio: { input: config.audio.input, sampleRate: config.audio.sampleRate, channels: config.audio.channels, codec: 'Opus' },
  });

  const signalingClient = instrumentSignaling(createSignalingClient({
    region: config.region,
    credentials,
    channelARN,
    endpoint: endpoints.WSS,
    role: Role.MASTER,
  }), monitor);

  signalingClient.on('open', () => {
    log.step('signaling websocket OPEN — waiting for a viewer to send an offer');
  });

  signalingClient.on('sdpOffer', async (offer, remoteClientId) => {
    const id = remoteClientId || 'anonymous-viewer';
    const peerLog = log.child(id);
    peerLog.step('SDP OFFER received from viewer');
    logSdp(peerLog, 'offer', offer);

    if (peers.has(id)) {
      peerLog.info('viewer re-offered — tearing down the previous session');
      closePeer(id);
    }

    peerLog.debug('creating RTCPeerConnection', {
      iceServers: iceServers.length,
      policy: config.forceTurn ? 'relay' : 'all',
    });
    const peerConnection = new RTCPeerConnection({
      iceServers,
      iceTransportPolicy: config.forceTurn ? 'relay' : 'all',
    });
    attachConnectionLogging(peerConnection, peerLog);

    monitor.peer(id, { kind: 'viewer', state: 'negotiating' });

    const audioOutput = new AudioOutput({
      mode: config.output.audio,
      dir: config.output.dir,
      label: `from-${id}`,
      device: config.output.audioDevice,
      debug: config.debug,
      onLevel: (level) => monitor.audioLevel(id, level),
    });
    const candidates = createCandidateQueue(peerConnection, peerLog);
    peerConnection.__candidates = candidates;

    const stopStats = startStatsReporter(peerConnection, peerLog, config.statsIntervalMs,
      (sample) => monitor.sample(id, sample));
    peers.set(id, { peerConnection, audioOutput, stopStats, log: peerLog });

    let sentCandidates = 0;
    peerConnection.onicecandidate = ({ candidate }) => {
      if (candidate) {
        sentCandidates += 1;
        peerLog.debug('local ICE candidate -> viewer', {
          ...summarizeCandidate(candidate), total: sentCandidates,
        });
        signalingClient.sendIceCandidate(candidate, remoteClientId);
      } else {
        peerLog.info('local ICE gathering complete', { candidatesSent: sentCandidates });
      }
    };

    // Inbound media — we only care about the viewer's microphone.
    peerConnection.ontrack = ({ track }) => {
      peerLog.step(`inbound ${track.kind} track from viewer`, { id: track.id });
      monitor.peer(id, { inbound: track.kind });
      if (track.kind === 'audio') audioOutput.attach(track);
      else peerLog.debug('ignoring inbound video (the panel only consumes audio)');
      track.onended = () => peerLog.info(`inbound ${track.kind} track ended`);
      track.onmute = () => peerLog.warn(`inbound ${track.kind} track muted (no packets arriving)`);
      track.onunmute = () => peerLog.info(`inbound ${track.kind} track unmuted`);
    };

    peerConnection.onconnectionstatechange = () => {
      const state = peerConnection.connectionState;
      peerLog.step(`connection state -> ${state}`);
      monitor.peer(id, { state, ice: peerConnection.iceConnectionState });
      if (state === 'connected') peerLog.info('media path established — audio/video should be flowing');
      if (state === 'failed') peerLog.error('connection failed — try KVS_FORCE_TURN=true');
      if (state === 'failed' || state === 'closed' || state === 'disconnected') closePeer(id);
    };

    try {
      // Set the remote description first so our tracks bind to the
      // transceivers the viewer already offered instead of adding new m-lines.
      peerLog.debug('setRemoteDescription(offer)');
      await peerConnection.setRemoteDescription(new RTCSessionDescription(offer));
      peerLog.info('remote description applied', { signalingState: peerConnection.signalingState });
      candidates.flush();

      peerLog.debug('adding local video + audio tracks');
      peerConnection.addTrack(videoSource.createTrack());
      peerConnection.addTrack(audioSource.createTrack());
      peerLog.debug('transceivers after addTrack', {
        directions: peerConnection.getTransceivers()
          .map((t) => `${(t.receiver.track && t.receiver.track.kind) || '?'}:${t.direction}`)
          .join(' '),
      });

      peerLog.step('creating SDP ANSWER');
      const answer = await peerConnection.createAnswer();
      await peerConnection.setLocalDescription(answer);
      logSdp(peerLog, 'answer', peerConnection.localDescription);

      signalingClient.sendSdpAnswer(
        { type: peerConnection.localDescription.type, sdp: peerConnection.localDescription.sdp },
        remoteClientId,
      );
      peerLog.step('SDP ANSWER sent — starting ICE');
    } catch (err) {
      peerLog.error('negotiation failed', { error: err.message });
      peerLog.debug(err.stack);
      closePeer(id);
    }
  });

  signalingClient.on('iceCandidate', (candidate, remoteClientId) => {
    const id = remoteClientId || 'anonymous-viewer';
    const entry = peers.get(id);
    if (!entry) {
      log.warn('remote ICE candidate for an unknown viewer — dropping', { clientId: id });
      return;
    }
    entry.peerConnection.__candidates.add(new RTCIceCandidate(candidate));
  });

  signalingClient.on('close', () => log.warn('signaling websocket CLOSED'));
  signalingClient.on('error', (err) => log.error('signaling error', { error: err.message || String(err) }));

  log.step('opening signaling websocket (SigV4-signed WSS)');
  signalingClient.open();

  const shutdown = () => {
    log.step('shutting down', { activePeers: peers.size });
    for (const id of [...peers.keys()]) closePeer(id);
    videoSource.stop();
    audioSource.stop();
    monitor.stop();
    try { signalingClient.close(); } catch { /* already closed */ }
    setTimeout(() => process.exit(0), 300).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  process.on('unhandledRejection', (err) =>
    log.error('unhandled rejection', { error: (err && err.message) || String(err) }));
}

function closePeer(id) {
  const entry = peers.get(id);
  if (!entry) return;
  peers.delete(id);
  if (activeMonitor) activeMonitor.removePeer(id);
  entry.log.info('closing peer connection', { remaining: peers.size });
  entry.stopStats();
  entry.audioOutput.stop();
  try { entry.peerConnection.close(); } catch { /* already closed */ }
}

main().catch((err) => {
  log.error('fatal', { error: err.message });
  console.error(err);
  process.exit(1);
});
