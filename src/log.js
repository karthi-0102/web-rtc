'use strict';

/**
 * Small leveled logger with timestamps, elapsed time and per-component tags,
 * plus helpers for the things that are genuinely hard to eyeball in WebRTC:
 * SDP shape, ICE candidate types, and rate-limited hot-path counters.
 *
 *   LOG_LEVEL=error|warn|info|debug|trace   (default: info, or debug if DEBUG_MEDIA)
 *   LOG_TIMESTAMPS=false                    to drop the clock column
 *   NO_COLOR=1                              to drop ANSI colors
 */

const LEVELS = { error: 0, warn: 1, info: 2, debug: 3, trace: 4 };

const envLevel = (process.env.LOG_LEVEL || '').toLowerCase();
const debugMedia = /^(1|true|yes|on)$/i.test(process.env.DEBUG_MEDIA || '');
const currentLevel = LEVELS[envLevel] !== undefined ? LEVELS[envLevel] : (debugMedia ? LEVELS.debug : LEVELS.info);

const showTime = !/^(0|false|no|off)$/i.test(process.env.LOG_TIMESTAMPS || '');
const useColor = !process.env.NO_COLOR && process.stdout.isTTY;

const START = Date.now();

const C = {
  reset: '\x1b[0m', dim: '\x1b[2m', bold: '\x1b[1m',
  red: '\x1b[31m', yellow: '\x1b[33m', green: '\x1b[32m',
  blue: '\x1b[34m', magenta: '\x1b[35m', cyan: '\x1b[36m', gray: '\x1b[90m',
};

const paint = (color, text) => (useColor ? `${color}${text}${C.reset}` : text);

const LEVEL_STYLE = {
  error: { label: 'ERROR', color: C.red },
  warn: { label: 'WARN ', color: C.yellow },
  info: { label: 'INFO ', color: C.green },
  debug: { label: 'DEBUG', color: C.cyan },
  trace: { label: 'TRACE', color: C.gray },
};

function clock() {
  const now = new Date();
  const wall = now.toISOString().slice(11, 23);
  const elapsed = ((Date.now() - START) / 1000).toFixed(2).padStart(7);
  return paint(C.gray, `${wall} +${elapsed}s`);
}

/** Render an object as `key=value` pairs so log lines stay greppable. */
function fields(details) {
  if (!details) return '';
  if (typeof details === 'string') return ' ' + paint(C.dim, details);
  const parts = Object.entries(details)
    .filter(([, v]) => v !== undefined && v !== null)
    .map(([k, v]) => {
      const value = typeof v === 'object' ? JSON.stringify(v) : String(v);
      return `${k}=${value}`;
    });
  return parts.length ? ' ' + paint(C.dim, parts.join(' ')) : '';
}

let stepCounter = 0;

/**
 * Extra destinations for every log line — used by the monitor dashboard so the
 * web view shows exactly what the terminal shows. Sinks get the structured
 * fields, not the ANSI-painted string.
 */
const sinks = [];

function addLogSink(sink) {
  sinks.push(sink);
  return () => {
    const index = sinks.indexOf(sink);
    if (index >= 0) sinks.splice(index, 1);
  };
}

function fanOut(entry) {
  for (const sink of sinks) {
    try {
      sink(entry);
    } catch { /* a broken sink must never break logging */ }
  }
}

function createLogger(component) {
  const tag = paint(C.magenta, `[${component}]`);

  const emit = (level, message, details, plain) => {
    if (LEVELS[level] > currentLevel) return;
    if (sinks.length) {
      fanOut({ level, component, message: plain !== undefined ? plain : message, details, ts: Date.now() });
    }
    const style = LEVEL_STYLE[level];
    const line = [
      showTime ? clock() : null,
      paint(style.color, style.label),
      tag,
      message,
    ].filter(Boolean).join(' ') + fields(details);
    (level === 'error' || level === 'warn' ? process.stderr : process.stdout).write(line + '\n');
  };

  return {
    component,

    /** A numbered milestone in the connection lifecycle — always shown at info. */
    step(message, details) {
      stepCounter += 1;
      const label = `STEP ${String(stepCounter).padStart(2, '0')} ▸ ${message}`;
      emit('info', paint(C.bold, label), details, label);
    },

    error: (message, details) => emit('error', message, details),
    warn: (message, details) => emit('warn', message, details),
    info: (message, details) => emit('info', message, details),
    debug: (message, details) => emit('debug', message, details),
    trace: (message, details) => emit('trace', message, details),

    /** Child logger for a single peer / sub-component. */
    child: (suffix) => createLogger(`${component}:${suffix}`),

    enabled: (level) => LEVELS[level] <= currentLevel,
  };
}

/* ------------------------------------------------------------- SDP helpers */

/**
 * Reduce an SDP blob to the parts that matter when debugging: one line per
 * media section with its direction, codecs and mid.
 */
function summarizeSdp(sdp) {
  const sections = [];
  let current = null;
  for (const raw of String(sdp || '').split(/\r?\n/)) {
    const line = raw.trim();
    if (line.startsWith('m=')) {
      const [kind, , proto, ...payloads] = line.slice(2).split(' ');
      current = { kind, proto, payloads, direction: 'sendrecv', mid: null, codecs: [], rtpmap: {} };
      sections.push(current);
      continue;
    }
    if (!current) continue;
    if (/^a=(sendrecv|sendonly|recvonly|inactive)$/.test(line)) current.direction = line.slice(2);
    else if (line.startsWith('a=mid:')) current.mid = line.slice(6);
    else if (line.startsWith('a=rtpmap:')) {
      const [pt, codec] = line.slice(9).split(' ');
      current.rtpmap[pt] = codec;
    }
  }
  return sections.map((s) => {
    const codecs = s.payloads.map((pt) => s.rtpmap[pt]).filter(Boolean);
    // Dedupe while keeping order; RTX/RED/FEC entries triple the list otherwise.
    const unique = [...new Set(codecs.map((c) => c.split('/')[0]))];
    return {
      kind: s.kind,
      mid: s.mid,
      direction: s.direction,
      codecs: unique.slice(0, 6).join(','),
    };
  });
}

function logSdp(logger, label, description) {
  const sdp = description && description.sdp;
  if (!sdp) return;
  const sections = summarizeSdp(sdp);
  logger.info(`${label}: ${sections.length} media section(s)`, {
    type: description.type,
    bytes: sdp.length,
  });
  for (const s of sections) {
    logger.info(`  m=${s.kind}`, { mid: s.mid, direction: s.direction, codecs: s.codecs });
  }
  logger.trace(`${label} full SDP:\n${sdp}`);
}

/* ------------------------------------------------------ ICE candidate helper */

/** Pull the interesting fields out of a raw `candidate:...` string. */
function summarizeCandidate(candidate) {
  const text = typeof candidate === 'string' ? candidate : (candidate && candidate.candidate) || '';
  const parts = text.replace(/^(a=)?candidate:/, '').split(' ');
  if (parts.length < 8) return { raw: text.slice(0, 80) };
  const typIndex = parts.indexOf('typ');
  return {
    type: typIndex >= 0 ? parts[typIndex + 1] : 'unknown', // host | srflx | prflx | relay
    protocol: parts[2],
    address: parts[4],
    port: parts[5],
    priority: parts[3],
  };
}

/* -------------------------------------------------------- rate-limited logs */

/**
 * Counter that reports at most once per `intervalMs` — for per-frame and
 * per-packet paths that would otherwise flood the terminal.
 */
function createRateLogger(logger, label, intervalMs = 5000) {
  let count = 0;
  let bytes = 0;
  let last = Date.now();
  let first = true;
  let extra = null;

  return {
    tick(byteCount = 0, details) {
      count += 1;
      bytes += byteCount;
      if (details) extra = details;
      if (first) {
        first = false;
        logger.info(`${label}: first item received`, details);
      }
      const now = Date.now();
      if (now - last < intervalMs) return;
      const seconds = (now - last) / 1000;
      logger.debug(`${label}: ${(count / seconds).toFixed(1)}/s`, {
        total: count,
        kbps: bytes ? ((bytes * 8) / seconds / 1000).toFixed(0) : undefined,
        ...extra,
      });
      count = 0;
      bytes = 0;
      last = now;
    },
  };
}

module.exports = {
  createLogger,
  addLogSink,
  createRateLogger,
  summarizeSdp,
  summarizeCandidate,
  logSdp,
  LEVELS,
  currentLevel,
};
