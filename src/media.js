'use strict';

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const wrtc = require('@roamhq/wrtc');
const { createLogger, createRateLogger } = require('./log');

const { RTCVideoSource, RTCAudioSource, RTCVideoSink, RTCAudioSink } = wrtc.nonstandard;

const FRAME_MS = 10; // wrtc's audio pipeline consumes exactly 10 ms per call

function spawnLogged(logger, command, args, { pipeStdin = true, onExit } = {}) {
  logger.debug(`spawn: ${command} ${args.join(' ')}`);
  const child = spawn(command, args, {
    stdio: [pipeStdin ? 'pipe' : 'ignore', pipeStdin ? 'ignore' : 'pipe', 'pipe'],
  });
  logger.debug('process started', { command, pid: child.pid });

  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => {
    for (const line of chunk.split('\n')) {
      const text = line.trim();
      if (!text) continue;
      // ffmpeg writes almost everything to stderr; only real errors deserve warn.
      if (/error|invalid|failed|no such|cannot|denied/i.test(text)) logger.warn(`${command}: ${text}`);
      else logger.debug(`${command}: ${text}`);
    }
  });
  child.on('error', (err) => {
    logger.error(`failed to start "${command}" — is it installed and on PATH?`, { error: err.message });
  });
  child.on('exit', (code, signal) => {
    logger.info(`${command} exited`, { pid: child.pid, code, signal: signal || undefined });
    if (onExit) onExit(code, signal);
  });
  if (child.stdin) {
    child.stdin.on('error', (err) => {
      logger.debug(`${command} stdin closed`, { error: err.code || err.message });
    });
  }
  return child;
}

function writeSafely(child, buffer) {
  if (child && child.stdin && child.stdin.writable) child.stdin.write(buffer);
}

/* ------------------------------------------------------------------ inputs */

function videoInputArgs({ input, width, height, fps }) {
  if (input === 'test') {
    return ['-re', '-f', 'lavfi', '-i', `testsrc2=size=${width}x${height}:rate=${fps}`];
  }
  if (input === 'screen') {
    return ['-f', 'x11grab', '-framerate', String(fps), '-video_size', `${width}x${height}`,
      '-i', process.env.DISPLAY || ':0.0'];
  }
  if (input.startsWith('/dev/video')) {
    return ['-f', 'v4l2', '-framerate', String(fps), '-video_size', `${width}x${height}`, '-i', input];
  }
  // A file or a network URL: -re paces it at wall-clock speed, and we loop it
  // so a short clip behaves like a live camera.
  return ['-re', '-stream_loop', '-1', '-i', input];
}

function audioInputArgs({ input, sampleRate }) {
  if (input === 'test') {
    return ['-re', '-f', 'lavfi', '-i', `sine=frequency=440:sample_rate=${sampleRate}`];
  }
  if (input.startsWith('alsa:')) return ['-f', 'alsa', '-i', input.slice('alsa:'.length)];
  if (input.startsWith('pulse:')) return ['-f', 'pulse', '-i', input.slice('pulse:'.length)];
  return ['-re', '-stream_loop', '-1', '-i', input];
}

/**
 * Outbound video: ffmpeg decodes whatever the input is into raw I420 frames,
 * which we hand straight to the WebRTC encoder.
 */
class VideoSource {
  constructor({ input, width, height, fps, debug }) {
    this.opts = { input, width, height, fps, debug };
    this.log = createLogger('video-in');
    this.source = new RTCVideoSource();
    this.frameSize = (width * height * 3) / 2; // I420 = 1.5 bytes per pixel
    this.buffer = Buffer.alloc(0);
    this.child = null;
    this.tracks = 0;
    this.taps = [];
    this.rate = createRateLogger(this.log, 'frames from ffmpeg', 5000);
  }

  /**
   * Observe every published frame (raw I420, `frameSize` bytes). Used by the
   * monitor dashboard for its preview; must never block or retain the buffer.
   */
  tap(fn) {
    this.taps.push(fn);
    return this;
  }

  start() {
    const { input, width, height, fps } = this.opts;
    this.log.step('starting outbound video source', {
      input, size: `${width}x${height}`, fps, frameBytes: this.frameSize,
    });

    const args = [
      '-loglevel', 'warning',
      ...videoInputArgs(this.opts),
      '-an',
      '-vf', `scale=${width}:${height},fps=${fps}`,
      '-pix_fmt', 'yuv420p',
      '-f', 'rawvideo', '-',
    ];
    this.child = spawnLogged(this.log, 'ffmpeg', args, {
      pipeStdin: false,
      onExit: () => this.log.warn('video input ended — the outbound stream will freeze'),
    });

    this.child.stdout.on('data', (chunk) => {
      this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
      while (this.buffer.length >= this.frameSize) {
        const frame = this.buffer.subarray(0, this.frameSize);
        this.buffer = this.buffer.subarray(this.frameSize);
        this.source.onFrame({
          width,
          height,
          data: new Uint8ClampedArray(frame), // copies out of the pooled Buffer
        });
        this.rate.tick(this.frameSize, { size: `${width}x${height}` });
        for (const tap of this.taps) {
          try {
            tap(frame, { width, height });
          } catch (err) {
            this.log.debug('frame tap threw', { error: err.message });
          }
        }
      }
    });
    return this;
  }

  /** One track per peer connection; they all share this single ffmpeg process. */
  createTrack() {
    this.tracks += 1;
    const track = this.source.createTrack();
    this.log.debug('created outbound video track', { id: track.id, tracks: this.tracks });
    return track;
  }

  stop() {
    this.log.info('stopping outbound video source');
    if (this.child) this.child.kill('SIGKILL');
    this.child = null;
  }
}

/**
 * Outbound audio: ffmpeg produces s16le PCM, and a drift-corrected timer feeds
 * the encoder exactly one 10 ms frame per tick (silence on underrun).
 */
class AudioSource {
  constructor({ input, sampleRate, channels, maxBufferMs, debug }) {
    this.opts = { input, sampleRate, channels, maxBufferMs, debug };
    this.log = createLogger('audio-in');
    this.source = new RTCAudioSource();
    this.samplesPerFrame = (sampleRate / 1000) * FRAME_MS;
    this.bytesPerFrame = this.samplesPerFrame * channels * 2;
    this.maxBytes = Math.max(this.bytesPerFrame, ((sampleRate * channels * 2) / 1000) * maxBufferMs);
    this.silence = Buffer.alloc(this.bytesPerFrame);
    this.queue = Buffer.alloc(0);
    this.child = null;
    this.timer = null;
    this.tracks = 0;
    this.frames = 0;
    this.underruns = 0;
    this.drops = 0;
    this.gotAudio = false;
    this.reportTimer = null;
  }

  start() {
    const { input, sampleRate, channels, maxBufferMs } = this.opts;
    this.log.step('starting outbound audio source', {
      input, sampleRate, channels,
      frameBytes: this.bytesPerFrame, maxBufferMs,
    });

    const args = [
      '-loglevel', 'warning',
      ...audioInputArgs(this.opts),
      '-vn',
      '-f', 's16le', '-acodec', 'pcm_s16le',
      '-ar', String(sampleRate), '-ac', String(channels), '-',
    ];
    this.child = spawnLogged(this.log, 'ffmpeg', args, {
      pipeStdin: false,
      onExit: () => this.log.warn('audio input ended — outbound audio is now silence'),
    });

    this.child.stdout.on('data', (chunk) => {
      if (!this.gotAudio) {
        this.gotAudio = true;
        this.log.info('first PCM chunk from ffmpeg', { bytes: chunk.length });
      }
      this.push(chunk);
    });

    // Drift-corrected pacing: schedule against an absolute clock so we don't
    // accumulate the few ms setTimeout always overshoots by.
    let next = Date.now();
    const tick = () => {
      next += FRAME_MS;
      this.emitFrame();
      const late = Date.now() - next;
      if (late > 50 && this.frames % 100 === 0) {
        this.log.debug('audio pacing running late', { lateMs: late });
      }
      this.timer = setTimeout(tick, Math.max(0, next - Date.now()));
    };
    this.timer = setTimeout(tick, FRAME_MS);

    // One line every 5 s summarising the outbound audio pipeline's health.
    this.reportTimer = setInterval(() => {
      this.log.debug('outbound audio pipeline', {
        frames: this.frames,
        queuedMs: Math.round((this.queue.length / (this.bytesPerFrame / FRAME_MS))),
        underruns: this.underruns,
        droppedChunks: this.drops,
      });
      this.underruns = 0;
      this.drops = 0;
    }, 5000);
    this.reportTimer.unref();

    return this;
  }

  push(chunk) {
    this.queue = this.queue.length ? Buffer.concat([this.queue, chunk]) : chunk;
    if (this.queue.length > this.maxBytes) {
      // Keep the newest audio: an intercom wants low latency, not completeness.
      this.drops += 1;
      this.queue = this.queue.subarray(this.queue.length - this.maxBytes);
    }
  }

  emitFrame() {
    let frame;
    if (this.queue.length >= this.bytesPerFrame) {
      frame = this.queue.subarray(0, this.bytesPerFrame);
      this.queue = this.queue.subarray(this.bytesPerFrame);
    } else {
      frame = this.silence;
      this.underruns += 1;
    }
    this.frames += 1;
    const samples = new Int16Array(
      frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength),
    );
    this.source.onData({
      samples,
      sampleRate: this.opts.sampleRate,
      bitsPerSample: 16,
      channelCount: this.opts.channels,
      numberOfFrames: this.samplesPerFrame,
    });
  }

  createTrack() {
    this.tracks += 1;
    const track = this.source.createTrack();
    this.log.debug('created outbound audio track', { id: track.id, tracks: this.tracks });
    return track;
  }

  stop() {
    this.log.info('stopping outbound audio source', { framesSent: this.frames });
    if (this.timer) clearTimeout(this.timer);
    if (this.reportTimer) clearInterval(this.reportTimer);
    this.timer = null;
    this.reportTimer = null;
    if (this.child) this.child.kill('SIGKILL');
    this.child = null;
  }
}

/* ----------------------------------------------------------------- outputs */

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

/**
 * Inbound audio: 10 ms PCM frames from the remote peer, sent to speakers
 * (ffplay), to a .wav file, or dropped.
 */
class AudioOutput {
  constructor({ mode, dir, label, device, debug, onLevel }) {
    this.mode = mode;
    // Optional 0..1 loudness callback (monitor dashboard's talk-back meter).
    this.onLevel = onLevel || null;
    this.levelAt = 0;
    this.dir = dir;
    this.label = label;
    this.device = device || null;
    this.log = createLogger(label);
    this.child = null;
    this.format = null;
    this.sink = null;
    this.rate = createRateLogger(this.log, 'inbound audio frames', 5000);
  }

  attach(track) {
    const silent = this.mode === 'none';
    if (silent && !this.onLevel) {
      this.log.info('AUDIO_OUT=none — inbound audio is being discarded');
      return;
    }
    if (silent) {
      this.log.info('AUDIO_OUT=none — not playing inbound audio, only metering it');
    } else {
      this.log.step(`attaching audio sink (mode=${this.mode})`, { trackId: track.id });
    }
    this.sink = new RTCAudioSink(track);
    this.sink.ondata = ({ samples, sampleRate, channelCount }) => {
      const buffer = Buffer.from(samples.buffer, samples.byteOffset, samples.byteLength);
      this.report(samples);
      if (silent) return;
      this.ensureProcess(sampleRate, channelCount);
      this.rate.tick(buffer.length, { sampleRate, channels: channelCount });
      writeSafely(this.child, buffer);
    };
  }

  /** RMS of one 10 ms frame, rate-limited to ~10 updates/s. */
  report(samples) {
    if (!this.onLevel) return;
    const now = Date.now();
    if (now - this.levelAt < 100) return;
    this.levelAt = now;
    let sum = 0;
    for (let i = 0; i < samples.length; i += 1) sum += samples[i] * samples[i];
    const rms = Math.sqrt(sum / Math.max(1, samples.length)) / 32768;
    this.onLevel(Math.min(1, rms));
  }

  ensureProcess(sampleRate, channelCount) {
    const format = `${sampleRate}:${channelCount}`;
    if (this.child && this.format === format) return;
    if (this.child) {
      this.log.info('inbound audio format changed, restarting sink', {
        from: this.format, to: format,
      });
      this.child.kill('SIGKILL');
    }
    this.format = format;

    // ffplay 7.1 dropped -ac ("Option not found", exit 1, silence). -ch_layout
    // is the replacement and both ffmpeg and ffplay accept it; the "Nc" form
    // means N channels of unspecified layout, which is what a WebRTC sink hands
    // us. -ar still works on both.
    const common = [
      '-f', 's16le', '-ar', String(sampleRate), '-ch_layout', `${channelCount}c`, '-i', '-',
    ];
    if (this.mode === 'play' && this.device) {
      // ffplay goes through SDL and always lands on the default sink, so pin a
      // specific speaker by muxing to PulseAudio/PipeWire directly instead.
      this.log.info('playing remote audio on a pinned sink', {
        device: this.device, sampleRate, channels: channelCount,
      });
      this.child = spawnLogged(this.log, 'ffmpeg',
        ['-loglevel', 'error', ...common, '-f', 'pulse', '-device', this.device, this.label]);
    } else if (this.mode === 'play') {
      this.log.info('playing remote audio through ffplay (system default sink)',
        { sampleRate, channels: channelCount });
      this.child = spawnLogged(this.log, 'ffplay',
        ['-loglevel', 'error', '-nodisp', '-autoexit', '-fflags', 'nobuffer', '-flags', 'low_delay', ...common]);
    } else {
      const file = path.join(ensureDir(this.dir), `${this.label}-${process.pid}.wav`);
      this.log.info('recording remote audio', { file, sampleRate, channels: channelCount });
      this.child = spawnLogged(this.log, 'ffmpeg',
        ['-loglevel', 'warning', ...common, '-y', file]);
    }
  }

  stop() {
    if (this.sink) {
      this.log.debug('stopping audio sink');
      this.sink.stop();
    }
    this.sink = null;
    if (this.child) {
      this.child.stdin.end();
      const child = this.child;
      setTimeout(() => child.kill('SIGKILL'), 500).unref();
    }
  }
}

/**
 * Inbound video: raw I420 frames to a window (ffplay) or an .mp4 file.
 *
 * WebRTC re-scales the stream when the network degrades, so the frame size can
 * change mid-call. Raw pipes have no way to signal that, so we restart the
 * downstream process (and open a new output file) whenever it happens.
 */
class VideoOutput {
  constructor({ mode, dir, label, fps, debug }) {
    this.mode = mode;
    this.dir = dir;
    this.label = label;
    this.fps = fps;
    this.log = createLogger(label);
    this.child = null;
    this.size = null;
    this.segment = 0;
    this.sink = null;
    this.rate = createRateLogger(this.log, 'inbound video frames', 5000);
  }

  attach(track) {
    if (this.mode === 'none') {
      this.log.info('VIDEO_OUT=none — inbound video is being discarded');
      return;
    }
    this.log.step(`attaching video sink (mode=${this.mode})`, { trackId: track.id });
    this.sink = new RTCVideoSink(track);
    this.sink.onframe = ({ frame }) => {
      this.ensureProcess(frame.width, frame.height);
      this.rate.tick(frame.data.byteLength, { size: `${frame.width}x${frame.height}` });
      writeSafely(this.child,
        Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength));
    };
  }

  ensureProcess(width, height) {
    const size = `${width}x${height}`;
    if (this.child && this.size === size) return;
    if (this.child) {
      this.log.info('remote resolution changed — restarting sink', { from: this.size, to: size });
      this.child.stdin.end();
    }
    this.size = size;

    const input = ['-f', 'rawvideo', '-pixel_format', 'yuv420p',
      '-video_size', size, '-framerate', String(this.fps), '-i', '-'];

    if (this.mode === 'play') {
      this.log.info('showing remote video in an ffplay window', { size });
      this.child = spawnLogged(this.log, 'ffplay',
        ['-loglevel', 'error', '-fflags', 'nobuffer', '-flags', 'low_delay', ...input]);
    } else {
      this.segment += 1;
      const file = path.join(ensureDir(this.dir), `${this.label}-${process.pid}-${this.segment}.mp4`);
      this.log.info('recording remote video', { file, size, assumedFps: this.fps });
      this.child = spawnLogged(this.log, 'ffmpeg',
        ['-loglevel', 'warning', ...input, '-c:v', 'libx264', '-preset', 'veryfast',
          '-pix_fmt', 'yuv420p', '-y', file]);
    }
  }

  stop() {
    if (this.sink) {
      this.log.debug('stopping video sink');
      this.sink.stop();
    }
    this.sink = null;
    if (this.child) {
      this.child.stdin.end();
      const child = this.child;
      setTimeout(() => child.kill('SIGKILL'), 1000).unref();
    }
  }
}

module.exports = { VideoSource, AudioSource, AudioOutput, VideoOutput, wrtc };
