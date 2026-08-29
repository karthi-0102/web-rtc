'use strict';

/**
 * Node-side wrapper around src/gst_peer.py.
 *
 * We use GStreamer rather than @roamhq/wrtc for the ingestion path because KVS
 * WebRTC ingestion requires H.264, which wrtc cannot encode. One helper process
 * owns the capture and encode; peers are added and removed against it.
 */

const { spawn } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

const HELPER = path.join(__dirname, 'gst_peer.py');

/**
 * Convert GetIceServerConfig output into what webrtcbin expects:
 * one stun://host:port, plus turn(s)://user:pass@host:port URIs.
 */
function toGstIceServers(iceServers) {
  let stunServer = null;
  const turnServers = [];

  for (const entry of iceServers) {
    const urls = Array.isArray(entry.urls) ? entry.urls : [entry.urls];
    for (const url of urls) {
      if (url.startsWith('stun:')) {
        if (!stunServer) stunServer = `stun://${url.slice('stun:'.length)}`;
        continue;
      }
      const match = /^(turns?):(.+)$/.exec(url);
      if (!match || !entry.username) continue;
      const [, scheme, rest] = match;
      // KVS usernames and passwords contain ':', '+' and '/', all of which
      // would otherwise be parsed as URI syntax.
      const user = encodeURIComponent(entry.username);
      const pass = encodeURIComponent(entry.credential);
      turnServers.push(`${scheme}://${user}:${pass}@${rest}`);
    }
  }
  return { stunServer, turnServers };
}

/**
 * Emits, all carrying the peer id as first argument:
 *   'answer'  (id, sdp)
 *   'ice'     (id, {candidate, sdpMLineIndex})
 *   'state'   (id, {connection?, ice?})
 * and process-wide: 'ready', 'fatal' (msg), 'exit', 'preview' (jpegBuffer).
 */
class GstMedia extends EventEmitter {
  constructor({ media, iceServers, logger, audioOutPipeline, previewFps = 0 }) {
    super();
    this.log = logger;
    this.ready = false;
    this.closed = false;

    const { stunServer, turnServers } = toGstIceServers(iceServers);
    const config = {
      videoInput: media.video.input,
      width: media.video.width,
      height: media.video.height,
      fps: media.video.fps,
      videoBitrateKbps: media.video.bitrateKbps,
      audioInput: media.audio.input,
      channels: media.audio.channels,
      audioBitrate: media.audio.bitrate,
      audioOutPipeline: audioOutPipeline || null,
      stunServer,
      turnServers,
      // >0 makes the helper emit JPEG thumbnails of the published video.
      previewFps,
    };
    this.log.step('starting GStreamer media server (H.264 + Opus)', {
      video: config.videoInput,
      size: `${config.width}x${config.height}`,
      fps: config.fps,
      stun: Boolean(stunServer),
      turn: turnServers.length,
    });

    this.child = spawn('python3', [HELPER, JSON.stringify(config)],
      { stdio: ['pipe', 'pipe', 'pipe'] });

    this._buffer = '';
    this.child.stdout.setEncoding('utf8');
    this.child.stdout.on('data', (chunk) => this._onData(chunk));

    this.child.stderr.setEncoding('utf8');
    this.child.stderr.on('data', (chunk) => {
      for (const line of chunk.split('\n')) {
        if (line.trim()) this.log.warn(`gst: ${line.trim()}`);
      }
    });

    this.child.on('exit', (code, signal) => {
      this.log.warn('GStreamer helper exited', { code, signal: signal || undefined });
      this.emit('exit', code);
    });
    this.child.on('error', (err) => this.log.error(
      'could not start python3 — is python3-gi + gstreamer1.0-plugins-bad installed?',
      { error: err.message },
    ));
    this.child.stdin.on('error', () => { /* helper gone; writes will fail */ });
  }

  _onData(chunk) {
    this._buffer += chunk;
    let index;
    while ((index = this._buffer.indexOf('\n')) >= 0) {
      const line = this._buffer.slice(0, index).trim();
      this._buffer = this._buffer.slice(index + 1);
      if (!line) continue;
      try {
        this._dispatch(JSON.parse(line));
      } catch {
        this.log.warn('unparseable line from helper', { raw: line.slice(0, 120) });
      }
    }
  }

  _dispatch(msg) {
    switch (msg.type) {
      case 'log': {
        const fn = this.log[msg.level] || this.log.debug;
        fn.call(this.log, msg.msg, msg.fields);
        break;
      }
      case 'ready':
        this.ready = true;
        this.emit('ready');
        break;
      case 'fatal':
        this.log.error('GStreamer pipeline failed fatally', { error: msg.msg });
        this.emit('fatal', msg.msg);
        break;
      case 'answer':
        this.emit('answer', msg.id, msg.sdp);
        break;
      case 'ice':
        this.emit('ice', msg.id, {
          candidate: msg.candidate, sdpMLineIndex: msg.sdpMLineIndex,
        });
        break;
      case 'state':
        this.emit('state', msg.id, msg);
        break;
      case 'preview':
        this.emit('preview', Buffer.from(msg.jpeg, 'base64'));
        break;
      default:
        this.log.debug('unhandled helper message', { type: msg.type });
    }
  }

  _send(obj) {
    if (this.child.stdin.writable) this.child.stdin.write(`${JSON.stringify(obj)}\n`);
  }

  /** Wait for the pipeline to reach PLAYING (or fail). */
  whenReady(timeoutMs = 15000) {
    if (this.ready) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('GStreamer pipeline did not start within 15s')), timeoutMs);
      timer.unref();
      this.once('ready', () => { clearTimeout(timer); resolve(); });
      this.once('fatal', (msg) => { clearTimeout(timer); reject(new Error(msg)); });
    });
  }

  addPeer(id, { recvAudio = true } = {}) {
    this._send({ type: 'add_peer', id, recvAudio });
  }

  setRemoteOffer(id, sdp) {
    this._send({ type: 'offer', id, sdp });
  }

  addIceCandidate(id, candidate, sdpMLineIndex) {
    this._send({ type: 'ice', id, candidate, sdpMLineIndex: sdpMLineIndex || 0 });
  }

  removePeer(id) {
    this._send({ type: 'remove_peer', id });
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    this._send({ type: 'stop' });
    const child = this.child;
    setTimeout(() => child.kill('SIGKILL'), 1500).unref();
  }
}

module.exports = { GstMedia, toGstIceServers };
