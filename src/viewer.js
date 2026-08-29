'use strict';

/**
 * KVS WebRTC VIEWER — the operator side.
 *
 *   receives: video + audio from the master (window via ffplay, or recorded)
 *   sends    : audio back to the master (mic, file, or a synthetic tone)
 *
 * A viewer creates the offer, so it decides the shape of the session: one
 * recvonly video transceiver plus one sendrecv audio track for talk-back.
 */

const { randomUUID } = require('crypto');

const { config, resolveCredentials, logConfig } = require('./config');
const {
  describeChannel, createSignalingClient, createCandidateQueue, attachConnectionLogging, Role,
} = require('./kvs');
const { AudioSource, AudioOutput, VideoOutput, wrtc } = require('./media');
const { createLogger, logSdp, summarizeCandidate } = require('./log');
const { startStatsReporter } = require('./stats');

const { RTCPeerConnection, RTCSessionDescription, RTCIceCandidate } = wrtc;

const log = createLogger('viewer');

async function main() {
  log.info('KVS WebRTC viewer starting');
  logConfig('VIEWER');

  const credentials = await resolveCredentials();
  // KVS client ids allow [a-zA-Z0-9_.-] only, and must be unique per viewer.
  const clientId = process.env.KVS_CLIENT_ID || `viewer-${randomUUID().slice(0, 8)}`;
  log.info('client id', { clientId });

  const { channelARN, endpoints, iceServers } = await describeChannel({
    region: config.region,
    credentials,
    channelName: config.channelName,
    role: Role.VIEWER,
  });

  log.step('creating RTCPeerConnection', {
    iceServers: iceServers.length,
    policy: config.forceTurn ? 'relay' : 'all',
  });
  const peerConnection = new RTCPeerConnection({
    iceServers,
    iceTransportPolicy: config.forceTurn ? 'relay' : 'all',
  });
  attachConnectionLogging(peerConnection, log);
  const candidates = createCandidateQueue(peerConnection, log);

  const videoOutput = new VideoOutput({
    mode: config.output.video,
    dir: config.output.dir,
    label: 'master-video',
    fps: config.video.fps,
    debug: config.debug,
  });
  const audioOutput = new AudioOutput({
    mode: config.output.audio,
    dir: config.output.dir,
    label: 'master-audio',
    device: config.output.audioDevice,
    debug: config.debug,
  });

  // Talk-back source. Point AUDIO_INPUT at "alsa:default"/"pulse:default" for a
  // real microphone; "test" emits a 440 Hz tone, handy for verifying the path.
  const audioSource = new AudioSource({ ...config.audio, debug: config.debug }).start();

  const signalingClient = createSignalingClient({
    region: config.region,
    credentials,
    channelARN,
    endpoint: endpoints.WSS,
    role: Role.VIEWER,
    clientId,
  });

  const stopStats = startStatsReporter(peerConnection, log, config.statsIntervalMs);

  let sentCandidates = 0;
  peerConnection.onicecandidate = ({ candidate }) => {
    if (candidate) {
      sentCandidates += 1;
      log.debug('local ICE candidate -> master', {
        ...summarizeCandidate(candidate), total: sentCandidates,
      });
      signalingClient.sendIceCandidate(candidate);
    } else {
      log.info('local ICE gathering complete', { candidatesSent: sentCandidates });
    }
  };

  peerConnection.ontrack = ({ track }) => {
    log.step(`inbound ${track.kind} track from master`, { id: track.id });
    if (track.kind === 'video') videoOutput.attach(track);
    else audioOutput.attach(track);
    track.onended = () => log.info(`inbound ${track.kind} track ended`);
    track.onmute = () => log.warn(`inbound ${track.kind} track muted (no packets arriving)`);
    track.onunmute = () => log.info(`inbound ${track.kind} track unmuted`);
  };

  peerConnection.onconnectionstatechange = () => {
    const state = peerConnection.connectionState;
    log.step(`connection state -> ${state}`);
    if (state === 'connected') log.info('media path established — expect video and audio shortly');
    if (state === 'failed') log.error('connection failed — try KVS_FORCE_TURN=true');
  };

  signalingClient.on('open', async () => {
    log.step('signaling websocket OPEN — building the offer');
    try {
      // Video is receive-only; audio is bidirectional (addTrack defaults to
      // sendrecv), which is what gives us master audio + talk-back on one m-line.
      log.debug('adding recvonly video transceiver');
      peerConnection.addTransceiver('video', { direction: 'recvonly' });
      log.debug('adding sendrecv audio track (talk-back)');
      peerConnection.addTrack(audioSource.createTrack());

      const offer = await peerConnection.createOffer({
        offerToReceiveAudio: true,
        offerToReceiveVideo: true,
      });
      await peerConnection.setLocalDescription(offer);
      logSdp(log, 'offer', peerConnection.localDescription);

      signalingClient.sendSdpOffer({
        type: peerConnection.localDescription.type,
        sdp: peerConnection.localDescription.sdp,
      });
      log.step('SDP OFFER sent — waiting for the master to answer');
    } catch (err) {
      log.error('failed to create/send offer', { error: err.message });
      log.debug(err.stack);
    }
  });

  signalingClient.on('sdpAnswer', async (answer) => {
    log.step('SDP ANSWER received from master');
    logSdp(log, 'answer', answer);
    try {
      await peerConnection.setRemoteDescription(new RTCSessionDescription(answer));
      log.info('remote description applied', { signalingState: peerConnection.signalingState });
      candidates.flush();
    } catch (err) {
      log.error('failed to apply answer', { error: err.message });
    }
  });

  signalingClient.on('iceCandidate', (candidate) => {
    candidates.add(new RTCIceCandidate(candidate));
  });

  signalingClient.on('close', () => log.warn('signaling websocket CLOSED'));
  signalingClient.on('error', (err) => log.error('signaling error', { error: err.message || String(err) }));

  log.step('opening signaling websocket (SigV4-signed WSS)');
  signalingClient.open();

  // If nothing has connected after a while, say so instead of sitting silent.
  const watchdog = setTimeout(() => {
    if (peerConnection.connectionState !== 'connected') {
      log.warn('still not connected after 30s', {
        connection: peerConnection.connectionState,
        ice: peerConnection.iceConnectionState,
        signaling: peerConnection.signalingState,
        hint: 'is the master running against the same channel and region?',
      });
    }
  }, 30000);
  watchdog.unref();

  const shutdown = () => {
    log.step('shutting down');
    clearTimeout(watchdog);
    stopStats();
    audioSource.stop();
    videoOutput.stop();
    audioOutput.stop();
    try { peerConnection.close(); } catch { /* already closed */ }
    try { signalingClient.close(); } catch { /* already closed */ }
    setTimeout(() => process.exit(0), 500).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  process.on('unhandledRejection', (err) =>
    log.error('unhandled rejection', { error: (err && err.message) || String(err) }));
}

main().catch((err) => {
  log.error('fatal', { error: err.message });
  console.error(err);
  process.exit(1);
});
