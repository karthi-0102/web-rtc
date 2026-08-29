'use strict';

/**
 * Periodic RTCPeerConnection stats: which candidate pair won, how much media is
 * actually moving, and whether packets are being lost. This is usually the
 * fastest way to tell "connected but silent" from "not connected".
 */

function num(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function collect(report) {
  const byId = new Map();
  report.forEach((entry) => byId.set(entry.id, entry));

  const out = { outbound: [], inbound: [], pair: null };

  report.forEach((entry) => {
    if (entry.type === 'outbound-rtp' && !entry.isRemote) {
      out.outbound.push({
        kind: entry.kind || entry.mediaType,
        bytes: num(entry.bytesSent),
        packets: num(entry.packetsSent),
        frames: num(entry.framesEncoded),
        width: entry.frameWidth,
        height: entry.frameHeight,
      });
    } else if (entry.type === 'inbound-rtp' && !entry.isRemote) {
      out.inbound.push({
        kind: entry.kind || entry.mediaType,
        bytes: num(entry.bytesReceived),
        packets: num(entry.packetsReceived),
        lost: num(entry.packetsLost),
        jitter: num(entry.jitter),
        frames: num(entry.framesDecoded),
        width: entry.frameWidth,
        height: entry.frameHeight,
      });
    } else if (entry.type === 'candidate-pair') {
      const isSelected = entry.selected === true || entry.nominated === true;
      if (isSelected && entry.state === 'succeeded' && !out.pair) {
        const local = byId.get(entry.localCandidateId);
        const remote = byId.get(entry.remoteCandidateId);
        out.pair = {
          local: local ? `${local.candidateType}/${local.protocol || ''}` : '?',
          remote: remote ? `${remote.candidateType}/${remote.protocol || ''}` : '?',
          rttMs: entry.currentRoundTripTime ? Math.round(entry.currentRoundTripTime * 1000) : undefined,
          availableKbps: entry.availableOutgoingBitrate
            ? Math.round(entry.availableOutgoingBitrate / 1000)
            : undefined,
        };
      }
    }
  });

  return out;
}

/** The kind is already in the log message, so keep it out of the fields. */
function omitKind({ kind, ...rest }) {
  return rest;
}

function rate(current, previous, seconds) {
  if (!previous || seconds <= 0) return undefined;
  return Math.round(((current - previous) * 8) / seconds / 1000); // kbps
}

/**
 * Start logging stats every `intervalMs`. Returns a stop() function.
 */
function startStatsReporter(peerConnection, logger, intervalMs = 5000, onSample = null) {
  let previous = null;
  let previousAt = Date.now();
  let announcedPair = false;

  const timer = setInterval(async () => {
    if (peerConnection.connectionState === 'closed') return;
    let report;
    try {
      report = await peerConnection.getStats();
    } catch (err) {
      logger.debug('getStats failed', { error: err.message });
      return;
    }

    const now = Date.now();
    const seconds = (now - previousAt) / 1000;
    const snapshot = collect(report);

    if (snapshot.pair && !announcedPair) {
      announcedPair = true;
      logger.info('selected candidate pair', snapshot.pair);
      if (snapshot.pair.local.startsWith('relay') || snapshot.pair.remote.startsWith('relay')) {
        logger.info('media is going through the KVS TURN relay (not peer-to-peer)');
      }
    }

    const tx = snapshot.outbound.map((track) => {
      const before = previous && previous.outbound.find((t) => t.kind === track.kind);
      return {
        kind: track.kind,
        kbps: rate(track.bytes, before && before.bytes, seconds),
        packets: track.packets,
        fps: before && track.frames
          ? Math.round((track.frames - before.frames) / seconds)
          : undefined,
        size: track.width ? `${track.width}x${track.height}` : undefined,
      };
    });
    for (const track of tx) logger.info(`tx ${track.kind}`, omitKind(track));

    const rx = snapshot.inbound.map((track) => {
      const before = previous && previous.inbound.find((t) => t.kind === track.kind);
      return {
        kind: track.kind,
        kbps: rate(track.bytes, before && before.bytes, seconds),
        packets: track.packets,
        lost: track.lost || undefined,
        jitterMs: track.jitter ? Math.round(track.jitter * 1000) : undefined,
        fps: before && track.frames
          ? Math.round((track.frames - before.frames) / seconds)
          : undefined,
        size: track.width ? `${track.width}x${track.height}` : undefined,
      };
    });
    for (const track of rx) logger.info(`rx ${track.kind}`, omitKind(track));

    if (!snapshot.outbound.length && !snapshot.inbound.length) {
      logger.debug('no RTP flowing yet');
    }

    if (onSample) {
      try {
        onSample({ tx, rx, pair: snapshot.pair });
      } catch { /* a monitor must never break the session */ }
    }

    previous = snapshot;
    previousAt = now;
  }, intervalMs);

  timer.unref();
  return () => clearInterval(timer);
}

module.exports = { startStatsReporter };
