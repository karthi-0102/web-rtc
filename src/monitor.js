'use strict';

/**
 * Local monitoring window for a running master.
 *
 * Everything a master does is already logged to the terminal, but "is video
 * actually going out, and what did AWS just say to me?" is hard to answer from
 * scrolling text. This serves a single-page dashboard over plain HTTP and pushes
 * live updates over a WebSocket:
 *
 *   - a preview of the exact video frames being published (also at
 *     /preview.jpg and /preview.mjpg for players outside the browser)
 *   - every signaling message in and out (SDP offers/answers, ICE candidates)
 *   - per-peer connection state, the winning ICE pair, and tx/rx bitrates
 *   - the log stream, so nothing that the terminal shows is missing here
 *
 * It is read-only: no control surface, nothing that can change the session.
 * Bound to 127.0.0.1 by default because the feed is the operator's live video.
 *
 *   MONITOR=false      disable entirely
 *   MONITOR_PORT=8088  port to listen on
 *   MONITOR_HOST=...   bind address (default 127.0.0.1)
 *   MONITOR_PREVIEW_FPS=4
 */

const http = require('http');
const os = require('os');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const { WebSocketServer } = require('ws');
const { createLogger, addLogSink, summarizeSdp, summarizeCandidate } = require('./log');

const UI_FILE = path.join(__dirname, 'monitor-ui.html');
const MAX_EVENTS = 400;

const SOI = Buffer.from([0xff, 0xd8, 0xff]);
const EOI = Buffer.from([0xff, 0xd9]);

function bool(value, fallback) {
  if (value === undefined || value === '') return fallback;
  return /^(1|true|yes|on)$/i.test(String(value));
}

function num(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/**
 * Raw I420 frames in, JPEG frames out, at a few frames per second — enough to
 * see that the picture is live without competing with the encoder for CPU.
 */
class PreviewEncoder {
  constructor({ width, height, fps, previewFps, logger, onJpeg }) {
    this.width = width;
    this.height = height;
    this.previewFps = Math.max(1, previewFps);
    this.everyNth = Math.max(1, Math.round(fps / this.previewFps));
    this.log = logger;
    this.onJpeg = onJpeg;
    this.child = null;
    this.buffer = Buffer.alloc(0);
    this.seen = 0;
    this.frames = 0;
    this.enabled = true;
  }

  start() {
    const args = [
      '-loglevel', 'error',
      '-f', 'rawvideo', '-pixel_format', 'yuv420p',
      '-video_size', `${this.width}x${this.height}`,
      '-framerate', String(this.previewFps),
      '-i', '-',
      '-vf', 'scale=480:-2',
      '-q:v', '7',
      '-f', 'mjpeg', '-',
    ];
    this.log.debug(`preview encoder: ffmpeg ${args.join(' ')}`);
    this.child = spawn('ffmpeg', args, { stdio: ['pipe', 'pipe', 'pipe'] });
    this.child.stderr.setEncoding('utf8');
    this.child.stderr.on('data', (chunk) => {
      const text = chunk.trim();
      if (text) this.log.debug(`preview ffmpeg: ${text}`);
    });
    this.child.on('error', (err) => {
      this.enabled = false;
      this.log.warn('preview encoder could not start — no video thumbnail', { error: err.message });
    });
    this.child.on('exit', (code) => {
      this.enabled = false;
      this.log.debug('preview encoder exited', { code });
    });
    this.child.stdin.on('error', () => { this.enabled = false; });
    this.child.stdout.on('data', (chunk) => this.consume(chunk));
    return this;
  }

  /** Split the concatenated JPEG stream ffmpeg writes into individual frames. */
  consume(chunk) {
    this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
    for (;;) {
      const start = this.buffer.indexOf(SOI);
      if (start < 0) {
        if (this.buffer.length > 4 << 20) this.buffer = Buffer.alloc(0);
        return;
      }
      const end = this.buffer.indexOf(EOI, start + SOI.length);
      if (end < 0) {
        if (start > 0) this.buffer = this.buffer.subarray(start);
        return;
      }
      const jpeg = this.buffer.subarray(start, end + EOI.length);
      this.buffer = this.buffer.subarray(end + EOI.length);
      this.frames += 1;
      this.onJpeg(Buffer.from(jpeg));
    }
  }

  /** Called for every published frame; most are dropped on the floor. */
  write(frameBuffer) {
    if (!this.enabled || !this.child) return;
    this.seen += 1;
    if (this.seen % this.everyNth !== 0) return;
    const { stdin } = this.child;
    // Never let a slow preview back-pressure the media path.
    if (stdin.writable && stdin.writableLength < 8 * frameBuffer.length) stdin.write(frameBuffer);
  }

  stop() {
    this.enabled = false;
    if (this.child) this.child.kill('SIGKILL');
    this.child = null;
  }
}

class Monitor {
  constructor({ role, config, port, host, previewFps }) {
    this.role = role;
    this.config = config;
    this.port = port;
    this.host = host;
    this.previewFps = previewFps;
    this.log = createLogger('monitor');
    this.startedAt = Date.now();

    this.events = [];
    this.seq = 0;
    this.peers = new Map();   // id -> peer record shown in the UI
    this.stats = new Map();   // id -> last stats sample
    this.media = { video: null, audio: null, publishedFrames: 0, inboundAudioLevel: 0 };
    this.lastFrame = null;
    this.lastFrameAt = 0;
    this.frameCount = 0;

    this.server = null;
    this.wss = null;
    this.clients = new Set();
    this.mjpegClients = new Set(); // MJPEG writers, for /preview.mjpg
    this.preview = null;
    this.url = null;
  }

  /* ------------------------------------------------------------- transport */

  start() {
    this.server = http.createServer((req, res) => this.handleHttp(req, res));
    this.wss = new WebSocketServer({ server: this.server });

    this.wss.on('connection', (socket) => {
      this.clients.add(socket);
      this.log.info('monitor window connected', { viewers: this.clients.size });
      socket.on('close', () => {
        this.clients.delete(socket);
        this.log.debug('monitor window disconnected', { viewers: this.clients.size });
      });
      socket.on('error', () => this.clients.delete(socket));
      this.sendSnapshot(socket);
    });

    // A listen failure must never take the master down: the dashboard is an
    // observer. ws re-emits the HTTP server's errors on the WebSocketServer, so
    // both need a handler or the second one throws.
    const onServerError = (err) => {
      if (err.code === 'EADDRINUSE') {
        this.log.warn(`monitor port ${this.port} is already in use — dashboard disabled`, {
          hint: 'another master is probably running; set MONITOR_PORT to a free port',
        });
      } else {
        this.log.warn('monitor server error', { error: err.message });
      }
      this.disable();
    };
    this.server.on('error', onServerError);
    this.wss.on('error', onServerError);

    this.server.listen(this.port, this.host, () => {
      const wide = this.host === '0.0.0.0' || this.host === '::';
      this.url = `http://${wide ? lanAddress() : this.host}:${this.port}`;
      this.log.step(`monitor window ready — open ${this.url}`);
      if (wide) {
        this.log.warn('monitor is reachable from the network and has no authentication — '
          + 'anyone who can reach this port sees the live video and the SDP (which carries '
          + 'TURN credentials)');
      }
    });

    addLogSink((entry) => this.onLog(entry));
    return this;
  }

  handleHttp(req, res) {
    const url = (req.url || '/').split('?')[0];
    if (url === '/' || url === '/index.html') {
      let html;
      try {
        html = fs.readFileSync(UI_FILE);
      } catch (err) {
        res.writeHead(500, { 'content-type': 'text/plain' });
        res.end(`monitor UI missing: ${err.message}`);
        return;
      }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
      res.end(html);
      return;
    }
    if (url === '/state') {
      res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify(this.snapshot(), null, 2));
      return;
    }
    if (url === '/preview.jpg') {
      if (!this.lastFrame) {
        res.writeHead(503, { 'content-type': 'text/plain' });
        res.end('no frame yet');
        return;
      }
      res.writeHead(200, { 'content-type': 'image/jpeg', 'cache-control': 'no-store' });
      res.end(this.lastFrame);
      return;
    }
    if (url === '/preview.mjpg') {
      this.streamMjpeg(res);
      return;
    }
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  }

  /**
   * multipart/x-mixed-replace MJPEG: what ffplay, VLC, mpv and a bare <img>
   * all understand, for watching the published video without the dashboard.
   */
  streamMjpeg(res) {
    const boundary = 'kvsframe';
    res.writeHead(200, {
      'content-type': `multipart/x-mixed-replace; boundary=${boundary}`,
      'cache-control': 'no-store',
      connection: 'close',
    });
    const send = (jpeg) => {
      if (res.writableEnded || res.destroyed) return;
      // Drop frames for a client that is not keeping up rather than buffering.
      if (res.writableLength > 4 * jpeg.length) return;
      res.write(`--${boundary}\r\nContent-Type: image/jpeg\r\n`
        + `Content-Length: ${jpeg.length}\r\n\r\n`);
      res.write(jpeg);
      res.write('\r\n');
    };
    this.mjpegClients.add(send);
    if (this.lastFrame) send(this.lastFrame);
    const drop = () => this.mjpegClients.delete(send);
    res.on('close', drop);
    res.on('error', drop);
    this.log.debug('MJPEG client attached', { clients: this.mjpegClients.size });
  }

  broadcast(message) {
    if (!this.clients.size) return;
    const payload = typeof message === 'string' ? message : JSON.stringify(message);
    for (const socket of this.clients) {
      if (socket.readyState === 1) {
        try { socket.send(payload); } catch { /* client is going away */ }
      }
    }
  }

  snapshot() {
    return {
      t: 'hello',
      role: this.role,
      startedAt: this.startedAt,
      config: this.config,
      media: this.media,
      frames: this.frameCount,
      lastFrameAgeMs: this.lastFrameAt ? Date.now() - this.lastFrameAt : null,
      peers: [...this.peers.values()],
      stats: Object.fromEntries(this.stats),
      events: this.events,
    };
  }

  sendSnapshot(socket) {
    try {
      socket.send(JSON.stringify(this.snapshot()));
      if (this.lastFrame) socket.send(this.lastFrame, { binary: true });
    } catch { /* client vanished mid-handshake */ }
  }

  /* ------------------------------------------------------------ ingestion */

  push(event) {
    this.seq += 1;
    const record = { seq: this.seq, ts: Date.now(), ...event };
    this.events.push(record);
    if (this.events.length > MAX_EVENTS) this.events.splice(0, this.events.length - MAX_EVENTS);
    this.broadcast(record);
    return record;
  }

  onLog({ level, component, message, details }) {
    // The monitor's own lines would echo forever.
    if (component === 'monitor') return;
    this.push({ t: 'log', level, component, message, details: normalizeDetails(details) });
  }

  /** A signaling message, in either direction. */
  signal({ dir, kind, peer, sdp, candidate, note }) {
    const event = { t: 'signal', dir, kind, peer: peer || null, note: note || null };
    if (sdp) {
      event.sections = summarizeSdp(sdp.sdp || sdp);
      event.sdp = String(sdp.sdp || sdp);
    }
    if (candidate) event.candidate = summarizeCandidate(candidate);
    this.push(event);
  }

  /** Create or update the row for one peer. */
  peer(id, patch) {
    const existing = this.peers.get(id) || {
      id, kind: 'viewer', state: 'new', ice: null, since: Date.now(), connectedAt: null,
    };
    const next = { ...existing, ...patch, id };
    if (patch.state === 'connected' && !existing.connectedAt) next.connectedAt = Date.now();
    this.peers.set(id, next);
    this.push({ t: 'peer', peer: next, peers: this.peers.size });
    return next;
  }

  removePeer(id) {
    const entry = this.peers.get(id);
    if (!entry) return;
    this.peers.delete(id);
    this.stats.delete(id);
    this.push({ t: 'peer-gone', peer: id, peers: this.peers.size, heldMs: Date.now() - entry.since });
  }

  /** One stats sample for a peer: tx/rx tracks plus the selected candidate pair. */
  sample(id, sample) {
    this.stats.set(id, { ...sample, ts: Date.now() });
    this.push({ t: 'stats', peer: id, ...sample });
  }

  /** Describe the media that is being published (shown above the preview). */
  setMedia(patch) {
    this.media = { ...this.media, ...patch };
    this.push({ t: 'media', media: this.media });
  }

  /** Inbound talk-back level, 0..1, so the UI can draw a meter. */
  audioLevel(id, level) {
    this.media.inboundAudioLevel = level;
    this.broadcast({ t: 'level', peer: id, level });
  }

  /* -------------------------------------------------------------- preview */

  /**
   * Wire a VideoSource-shaped object (anything with `tap(fn)`) into the preview
   * encoder. Returns the encoder, or null when preview is not possible.
   */
  attachVideoSource(videoSource, { width, height, fps }) {
    if (!videoSource || typeof videoSource.tap !== 'function') return null;
    this.preview = new PreviewEncoder({
      width, height, fps, previewFps: this.previewFps, logger: this.log,
      onJpeg: (jpeg) => this.frame(jpeg),
    }).start();
    videoSource.tap((frameBuffer) => {
      this.media.publishedFrames += 1;
      this.preview.write(frameBuffer);
    });
    return this.preview;
  }

  /** A ready-made JPEG preview frame (from ffmpeg here, or from GStreamer). */
  frame(jpeg) {
    if (this.enabled === false) return;
    this.lastFrame = jpeg;
    this.lastFrameAt = Date.now();
    this.frameCount += 1;
    for (const send of this.mjpegClients) send(jpeg);
    if (!this.clients.size) return;
    for (const socket of this.clients) {
      // Skip a client that is not draining; a stale thumbnail beats a stall.
      if (socket.readyState === 1 && socket.bufferedAmount < 2 * jpeg.length) {
        try { socket.send(jpeg, { binary: true }); } catch { /* going away */ }
      }
    }
  }

  /** Tear the server down but leave every method callable (no-ops from here). */
  disable() {
    this.enabled = false;
    this.url = null;
    if (this.preview) this.preview.stop();
    this.preview = null;
    for (const socket of this.clients) {
      try { socket.close(); } catch { /* already closed */ }
    }
    this.clients.clear();
    this.mjpegClients.clear();
    if (this.wss) {
      try { this.wss.close(); } catch { /* not listening */ }
      this.wss = null;
    }
    if (this.server) {
      try { this.server.close(); } catch { /* not listening */ }
      this.server = null;
    }
  }

  stop() {
    if (this.preview) this.preview.stop();
    for (const socket of this.clients) {
      try { socket.close(); } catch { /* already closed */ }
    }
    this.clients.clear();
    if (this.wss) this.wss.close();
    if (this.server) this.server.close();
  }
}

/** First non-loopback IPv4 address, so a wide bind prints a URL others can use. */
function lanAddress() {
  for (const list of Object.values(os.networkInterfaces())) {
    for (const iface of list || []) {
      if (iface.family === 'IPv4' && !iface.internal) return iface.address;
    }
  }
  return 'localhost';
}

function normalizeDetails(details) {
  if (details === undefined || details === null) return null;
  if (typeof details === 'string') return details;
  const out = {};
  for (const [key, value] of Object.entries(details)) {
    if (value === undefined || value === null) continue;
    out[key] = typeof value === 'object' ? JSON.stringify(value) : value;
  }
  return Object.keys(out).length ? out : null;
}

/**
 * A no-op with the same shape as Monitor, so callers never have to branch on
 * whether monitoring is switched on.
 */
function createNullMonitor() {
  const noop = () => {};
  return {
    enabled: false,
    url: null,
    start() { return this; },
    push: noop,
    signal: noop,
    peer: noop,
    removePeer: noop,
    sample: noop,
    setMedia: noop,
    audioLevel: noop,
    attachVideoSource: () => null,
    frame: noop,
    stop: noop,
  };
}

/**
 * Build the monitor for a master process. Honours MONITOR / MONITOR_PORT and
 * returns a null object when disabled, so the call sites stay unconditional.
 */
function createMonitor({ role, config }) {
  if (!bool(process.env.MONITOR, true)) return createNullMonitor();
  const monitor = new Monitor({
    role,
    config: {
      region: config.region,
      channel: config.channelName,
      video: config.video,
      audio: config.audio,
      output: config.output,
      forceTurn: config.forceTurn,
      pid: process.pid,
    },
    port: num(process.env.MONITOR_PORT, 8088),
    host: process.env.MONITOR_HOST || '0.0.0.0',
    previewFps: Math.max(1, num(process.env.MONITOR_PREVIEW_FPS, 4)),
  });
  monitor.enabled = true;
  return monitor.start();
}

/**
 * Mirror a KVS SignalingClient's traffic into the monitor: inbound events by
 * subscribing, outbound by wrapping the three send methods.
 */
function instrumentSignaling(signalingClient, monitor) {
  if (!monitor.enabled) return signalingClient;

  signalingClient.on('open', () => monitor.signal({ dir: 'in', kind: 'open', note: 'signaling websocket OPEN' }));
  signalingClient.on('close', () => monitor.signal({ dir: 'in', kind: 'close', note: 'signaling websocket CLOSED' }));
  signalingClient.on('error', (err) => monitor.signal({
    dir: 'in', kind: 'error', note: (err && err.message) || String(err),
  }));
  signalingClient.on('sdpOffer', (offer, peer) => monitor.signal({
    dir: 'in', kind: 'sdpOffer', peer: peer || '(none)', sdp: offer,
  }));
  signalingClient.on('sdpAnswer', (answer, peer) => monitor.signal({
    dir: 'in', kind: 'sdpAnswer', peer: peer || '(none)', sdp: answer,
  }));
  signalingClient.on('iceCandidate', (candidate, peer) => monitor.signal({
    dir: 'in', kind: 'iceCandidate', peer: peer || '(none)', candidate,
  }));

  for (const [method, kind] of [
    ['sendSdpOffer', 'sdpOffer'],
    ['sendSdpAnswer', 'sdpAnswer'],
    ['sendIceCandidate', 'iceCandidate'],
  ]) {
    const original = signalingClient[method];
    if (typeof original !== 'function') continue;
    signalingClient[method] = function instrumented(payload, peer, ...rest) {
      if (kind === 'iceCandidate') monitor.signal({ dir: 'out', kind, peer: peer || '(none)', candidate: payload });
      else monitor.signal({ dir: 'out', kind, peer: peer || '(none)', sdp: payload });
      return original.call(this, payload, peer, ...rest);
    };
  }
  return signalingClient;
}

module.exports = { createMonitor, instrumentSignaling, Monitor, PreviewEncoder };
