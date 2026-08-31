'use strict';

require('dotenv').config();

const { fromNodeProviderChain } = require('@aws-sdk/credential-providers');
const { createLogger } = require('./log');

const log = createLogger('config');

function bool(value, fallback) {
  if (value === undefined || value === '') return fallback;
  return /^(1|true|yes|on)$/i.test(String(value));
}

function num(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

const config = {
  region: process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1',
  channelName: process.env.KVS_CHANNEL_NAME || 'karthi-test',

  // Force media through the KVS TURN relays. Useful when the panel sits behind
  // a symmetric NAT and host/srflx candidates never pair up.
  forceTurn: bool(process.env.KVS_FORCE_TURN, false),

  video: {
    // "test" | "screen" | /dev/videoN | any file path or URL ffmpeg can open
    input: process.env.VIDEO_INPUT || 'test',
    width: num(process.env.VIDEO_WIDTH, 640),
    height: num(process.env.VIDEO_HEIGHT, 480),
    fps: num(process.env.VIDEO_FPS, 30),
    // H.264 target bitrate for the ingestion path (kbps). Sized for 720p30 on
    // zerolatency/constrained-baseline, which has no B-frames and no CABAC to
    // lean on: below ~2000 the picture goes visibly soft on any motion. Scale
    // it with pixel count -- roughly a quarter of this for 640x480.
    bitrateKbps: num(process.env.VIDEO_BITRATE_KBPS, 2500),
  },

  audio: {
    // "test" | "alsa:<device>" | "pulse:<device>" | any file path or URL
    input: process.env.AUDIO_INPUT || 'test',
    sampleRate: num(process.env.AUDIO_SAMPLE_RATE, 48000),
    channels: num(process.env.AUDIO_CHANNELS, 1),
    // Drop the oldest audio if the encoder falls behind, so latency stays bounded.
    maxBufferMs: num(process.env.AUDIO_MAX_BUFFER_MS, 400),
    // Opus target bitrate for the ingestion path (bits/sec).
    bitrate: num(process.env.AUDIO_BITRATE, 64000),
  },

  output: {
    // "play" (ffplay), "file" (record), or "none"
    audio: process.env.AUDIO_OUT || 'play',
    video: process.env.VIDEO_OUT || 'play',
    dir: process.env.OUT_DIR || './recordings',
    // Which speaker to play remote audio on. Unset = whatever the system
    // default sink is. Set to a PulseAudio/PipeWire sink node.name to pin it.
    audioDevice: process.env.AUDIO_OUT_DEVICE || null,
  },

  // Kinesis Video Stream that JoinStorageSession writes into. Must already be
  // wired to the channel via UpdateMediaStorageConfiguration.
  storageStreamName: process.env.KVS_STREAM_NAME || null,

  // Live playback latency budget for the GStreamer receiver.
  playout: {
    // webrtcbin's jitter buffer depth. Higher rides out jitter, lower plays
    // closer to live. Below ~50ms packet reordering starts costing frames.
    jitterMs: num(process.env.JITTER_LATENCY_MS, 100),
    // How much decoded backlog a sink branch may hold before it drops the
    // oldest buffer. This is the ceiling on how far behind live playback can
    // fall after a network stall.
    queueMs: num(process.env.PLAYOUT_QUEUE_MS, 200),
  },

  // Inbound talk-back audio on the ingest master. Both of these bound how much
  // delay the local playback path adds on top of whatever the network costs.
  talkback: {
    // Decoded backlog allowed before the oldest buffer is dropped. Without a
    // cap a burst of late packets is played out in full and the delay it
    // introduced is never recovered.
    queueMs: num(process.env.TALKBACK_QUEUE_MS, 60),
    // The sink's ring buffer, and so the floor on how late talk-back can be.
    // pulsesink defaults to 200ms, which dominates every other local term.
    sinkMs: num(process.env.TALKBACK_SINK_MS, 40),
  },

  // Burn the pipeline's running time into the published picture. The monitor
  // thumbnail and the viewer's window then carry the same stamp, and the gap
  // between them is the end-to-end delay, measurable by eye.
  clockOverlay: bool(process.env.VIDEO_CLOCK_OVERLAY, false),

  // How often to print RTP/ICE stats for a live connection.
  statsIntervalMs: num(process.env.STATS_INTERVAL_MS, 5000),

  // Print ffmpeg/ffplay stderr (also implies LOG_LEVEL=debug — see log.js).
  debug: bool(process.env.DEBUG_MEDIA, false),
};

/** Dump the effective configuration so a surprising run is explainable. */
function logConfig(role) {
  log.step(`configuration resolved (role=${role})`);
  log.info('aws', {
    region: config.region,
    channel: config.channelName,
    profile: process.env.AWS_PROFILE || 'kvs',
    forceTurn: config.forceTurn,
  });
  log.info('video in', config.video);
  log.info('audio in', config.audio);
  log.info('outputs', config.output);
  log.info('playout', config.playout);
  log.info('talkback', config.talkback);
  log.info('delay measurement', { clockOverlay: config.clockOverlay });
  log.debug('node', { version: process.version, pid: process.pid, platform: process.platform });
}

/**cond
 * KVS's SignalingClient wants plain static credentials, so we resolve the
 * standard provider chain (env vars -> shared config -> SSO -> IMDS) once.
 */
async function resolveCredentials() {
  log.step('resolving AWS credentials from the default provider chain');
  const started = Date.now();
  const credentials = await fromNodeProviderChain({ profile: process.env.AWS_PROFILE })();
  const keyId = credentials.accessKeyId || '';
  log.info('credentials resolved', {
    accessKeyId: `${keyId.slice(0, 4)}…${keyId.slice(-4)}`,
    temporary: Boolean(credentials.sessionToken),
    expiration: credentials.expiration ? credentials.expiration.toISOString() : '(none)',
    ms: Date.now() - started,
  });
  if (credentials.expiration) {
    const minutes = Math.round((credentials.expiration.getTime() - Date.now()) / 60000);
    if (minutes < 60) {
      log.warn(`credentials expire in ~${minutes} min; the session will not outlive them`);
    }
  }
  return {
    accessKeyId: credentials.accessKeyId,
    secretAccessKey: credentials.secretAccessKey,
    sessionToken: credentials.sessionToken,
  };
}

module.exports = { config, resolveCredentials, logConfig };
