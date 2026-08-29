'use strict';

/**
 * KVS storage-session VIEWER (the multiviewer path).
 *
 * With MediaStorageConfiguration ENABLED, viewers do NOT peer with the master
 * directly. The master streams into the Kinesis Video Stream, and each viewer
 * calls JoinStorageSessionAsViewer; AWS then offers media to that viewer and
 * performs the fan-out. That is what makes many viewers possible without the
 * master paying for each one.
 *
 * AWS sends the offer, so this side only ever answers -- the mirror image of
 * viewer.js, which offers to a master.
 */

const { randomUUID } = require('crypto');
const path = require('path');
const fs = require('fs');

const { config, resolveCredentials, logConfig } = require('./config');
const { describeChannel, createSignalingClient, Role } = require('./kvs');
const { toGstIceServers } = require('./gst');
const { createLogger, logSdp, summarizeCandidate } = require('./log');
const {
  KinesisVideoWebRTCStorageClient,
  JoinStorageSessionAsViewerCommand,
} = require('@aws-sdk/client-kinesis-video-webrtc-storage');
const { spawn } = require('child_process');

const log = createLogger('sview');
const HELPER = path.join(__dirname, 'gst_viewer.py');

async function main() {
  log.info('KVS storage-session viewer starting');
  logConfig('VIEWER (storage session)');

  const credentials = await resolveCredentials();
  const clientId = process.env.KVS_CLIENT_ID || `sview-${randomUUID().slice(0, 8)}`;
  log.info('client id', { clientId });

  const { channelARN, endpoints, iceServers } = await describeChannel({
    region: config.region,
    credentials,
    channelName: config.channelName,
    role: Role.VIEWER,
    protocols: ['WSS', 'HTTPS', 'WEBRTC'],
  });

  // ---------------------------------------------------------------- media
  const { stunServer, turnServers } = toGstIceServers(iceServers);
  if (config.output.video === 'file' || config.output.audio === 'file') {
    fs.mkdirSync(config.output.dir, { recursive: true });
  }
  const stamp = `${clientId}-${process.pid}`;
  const helperConfig = {
    stunServer,
    turnServers,
    // AWS relays viewer audio to the master and into the stream. Set
    // AUDIO_INPUT=none to join as a pure listener.
    audioInput: config.audio.input,
    audioBitrate: config.audio.bitrate,
    videoOut: config.output.video,
    audioOut: config.output.audio,
    audioDevice: config.output.audioDevice,
    videoFile: path.join(config.output.dir, `storage-${stamp}.mp4`),
    audioFile: path.join(config.output.dir, `storage-${stamp}.wav`),
  };

  log.step('starting GStreamer receiver (H.264 + Opus)', {
    videoOut: helperConfig.videoOut, audioOut: helperConfig.audioOut,
    talkBack: helperConfig.audioInput,
  });
  const child = spawn('python3', [HELPER, JSON.stringify(helperConfig)],
    { stdio: ['pipe', 'pipe', 'pipe'] });

  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => {
    for (const line of chunk.split('\n')) if (line.trim()) log.warn(`gst: ${line.trim()}`);
  });
  child.stdin.on('error', () => { /* helper gone */ });

  const send = (obj) => {
    if (child.stdin.writable) child.stdin.write(`${JSON.stringify(obj)}\n`);
  };

  // ------------------------------------------------------------ signaling
  const signalingClient = createSignalingClient({
    region: config.region,
    credentials,
    channelARN,
    endpoint: endpoints.WSS,
    role: Role.VIEWER,
    clientId,
  });

  let buffer = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (chunk) => {
    buffer += chunk;
    let index;
    while ((index = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, index).trim();
      buffer = buffer.slice(index + 1);
      if (!line) continue;
      let msg;
      try { msg = JSON.parse(line); } catch { continue; }

      switch (msg.type) {
        case 'log': {
          const fn = log[msg.level] || log.debug;
          fn.call(log, msg.msg, msg.fields);
          break;
        }
        case 'answer':
          logSdp(log, 'answer', { type: 'answer', sdp: msg.sdp });
          signalingClient.sendSdpAnswer({ type: 'answer', sdp: msg.sdp });
          log.step('SDP ANSWER sent to AWS');
          break;
        case 'ice':
          signalingClient.sendIceCandidate({
            candidate: msg.candidate, sdpMLineIndex: msg.sdpMLineIndex, sdpMid: null,
          });
          break;
        case 'media':
          log.step(`${msg.kind.toUpperCase()} stream arriving from the storage session`);
          break;
        case 'state':
          if (msg.ice) log.info(`ICE connection -> ${msg.ice}`);
          if (msg.connection) {
            log.step(`connection state -> ${msg.connection}`);
            if (msg.connection === 'connected') {
              log.info('LIVE — receiving the archived stream via AWS fan-out');
            }
          }
          break;
        default:
          break;
      }
    }
  });

  signalingClient.on('open', async () => {
    log.step('signaling websocket OPEN');
    const client = new KinesisVideoWebRTCStorageClient({
      region: config.region, credentials, endpoint: endpoints.WEBRTC,
    });
    log.step('JoinStorageSessionAsViewer — asking AWS to send us the stream', {
      clientId, endpoint: endpoints.WEBRTC,
    });
    try {
      await client.send(new JoinStorageSessionAsViewerCommand({
        channelArn: channelARN, clientId,
      }));
      log.info('accepted — expect an SDP offer from AWS shortly');
    } catch (err) {
      log.error('JoinStorageSessionAsViewer failed', { name: err.name, error: err.message });
    }
  });

  // AWS offers; we answer. The opposite of the plain viewer.js flow.
  signalingClient.on('sdpOffer', (offer) => {
    log.step('SDP OFFER received from AWS storage session');
    logSdp(log, 'offer', offer);
    send({ type: 'offer', sdp: offer.sdp });
  });

  signalingClient.on('iceCandidate', (candidate) => {
    log.debug('remote ICE candidate', summarizeCandidate(candidate));
    send({
      type: 'ice',
      candidate: candidate.candidate,
      sdpMLineIndex: candidate.sdpMLineIndex,
    });
  });

  signalingClient.on('close', () => log.warn('signaling websocket CLOSED'));
  signalingClient.on('error', (err) =>
    log.error('signaling error', { error: err.message || String(err) }));

  log.step('opening signaling websocket (SigV4-signed WSS)');
  signalingClient.open();

  const shutdown = () => {
    log.step('shutting down');
    send({ type: 'stop' });
    try { signalingClient.close(); } catch { /* already closed */ }
    // Give the helper time to flush EOS so recordings finalise their headers.
    setTimeout(() => { child.kill('SIGKILL'); process.exit(0); }, 4000).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  log.error('fatal', { error: err.message });
  console.error(err);
  process.exit(1);
});
