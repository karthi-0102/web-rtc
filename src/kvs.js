'use strict';

const { createLogger, summarizeCandidate } = require('./log');

const log = createLogger('kvs');

// The KVS WebRTC SDK is written for the browser: it reaches for a global
// WebSocket. Node 22 ships one, older runtimes do not, so polyfill from `ws`.
if (typeof globalThis.WebSocket === 'undefined') {
  globalThis.WebSocket = require('ws');
  log.debug('installed `ws` as the global WebSocket polyfill');
} else {
  log.debug('using the runtime\'s built-in global WebSocket');
}

const {
  KinesisVideoClient,
  DescribeSignalingChannelCommand,
  GetSignalingChannelEndpointCommand,
} = require('@aws-sdk/client-kinesis-video');
const {
  KinesisVideoSignalingClient,
  GetIceServerConfigCommand,
} = require('@aws-sdk/client-kinesis-video-signaling');
const { SignalingClient, Role } = require('amazon-kinesis-video-streams-webrtc');

/**
 * Look up the channel ARN, the WSS/HTTPS endpoints for our role, and the
 * STUN/TURN servers to hand to RTCPeerConnection.
 */
async function describeChannel({ region, credentials, channelName, role, protocols }) {
  const wanted = protocols || ['WSS', 'HTTPS'];
  const kinesisVideo = new KinesisVideoClient({ region, credentials });

  log.step(`DescribeSignalingChannel "${channelName}"`, { region });
  const describeStarted = Date.now();
  const { ChannelInfo } = await kinesisVideo.send(
    new DescribeSignalingChannelCommand({ ChannelName: channelName }),
  );
  if (!ChannelInfo) throw new Error(`Signaling channel "${channelName}" not found in ${region}`);
  const channelARN = ChannelInfo.ChannelARN;
  log.info('channel found', {
    arn: channelARN,
    status: ChannelInfo.ChannelStatus,
    type: ChannelInfo.ChannelType,
    ttlSeconds: ChannelInfo.SingleMasterConfiguration &&
      ChannelInfo.SingleMasterConfiguration.MessageTtlSeconds,
    ms: Date.now() - describeStarted,
  });
  if (ChannelInfo.ChannelStatus !== 'ACTIVE') {
    log.warn(`channel status is ${ChannelInfo.ChannelStatus}, not ACTIVE — signaling may fail`);
  }

  log.step(`GetSignalingChannelEndpoint for role ${role}`, { protocols: wanted.join(',') });
  const endpointStarted = Date.now();
  const { ResourceEndpointList } = await kinesisVideo.send(
    new GetSignalingChannelEndpointCommand({
      ChannelARN: channelARN,
      SingleMasterChannelEndpointConfiguration: {
        Protocols: wanted,
        Role: role,
      },
    }),
  );
  const endpoints = Object.fromEntries(
    (ResourceEndpointList || []).map((e) => [e.Protocol, e.ResourceEndpoint]),
  );
  log.info('endpoints received', { ...endpoints, ms: Date.now() - endpointStarted });
  if (!endpoints.WSS || !endpoints.HTTPS) {
    throw new Error('KVS did not return both WSS and HTTPS endpoints for this channel');
  }
  if (wanted.includes('WEBRTC') && !endpoints.WEBRTC) {
    throw new Error(
      'No WEBRTC endpoint returned. Enable media storage on the channel first:\n'
      + '  aws kinesisvideo update-media-storage-configuration --channel-arn <arn> \\\n'
      + '    --media-storage-configuration Status=ENABLED,StreamARN=<stream-arn>');
  }

  log.step('GetIceServerConfig (STUN/TURN credentials)', { endpoint: endpoints.HTTPS });
  const iceStarted = Date.now();
  const signaling = new KinesisVideoSignalingClient({
    region,
    credentials,
    endpoint: endpoints.HTTPS,
  });
  const { IceServerList } = await signaling.send(
    new GetIceServerConfigCommand({ ChannelARN: channelARN }),
  );

  const stunUrl = `stun:stun.kinesisvideo.${region}.amazonaws.com:443`;
  const iceServers = [{ urls: stunUrl }];
  log.info('ice server: STUN', { urls: stunUrl });
  for (const server of IceServerList || []) {
    iceServers.push({
      urls: server.Uris,
      username: server.Username,
      credential: server.Password,
    });
    log.info('ice server: TURN', {
      uris: (server.Uris || []).join(' '),
      ttlSeconds: server.Ttl,
      username: `${String(server.Username || '').slice(0, 12)}…`,
    });
  }
  log.debug('ice config complete', { servers: iceServers.length, ms: Date.now() - iceStarted });

  return { channelARN, endpoints, iceServers };
}

function createSignalingClient({
  region,
  credentials,
  channelARN,
  endpoint,
  role,
  clientId,
}) {
  log.step(`creating SignalingClient (role=${role})`, {
    clientId: clientId || '(master has no clientId)',
    endpoint,
  });
  return new SignalingClient({
    channelARN,
    channelEndpoint: endpoint,
    role,
    clientId: role === Role.VIEWER ? clientId : undefined,
    region,
    credentials,
    systemClockOffset: 0,
  });
}

/**
 * Trickle ICE can deliver remote candidates before the remote description is
 * set. Buffer them until the peer connection is ready to accept them.
 */
function createCandidateQueue(peerConnection, logger = log) {
  let ready = false;
  let added = 0;
  const pending = [];

  return {
    add(candidate) {
      const summary = summarizeCandidate(candidate);
      if (!ready) {
        pending.push(candidate);
        logger.debug('remote ICE candidate buffered (no remote description yet)', {
          ...summary, buffered: pending.length,
        });
        return;
      }
      added += 1;
      logger.debug('remote ICE candidate added', { ...summary, total: added });
      peerConnection.addIceCandidate(candidate).catch((err) => {
        logger.warn('failed to add remote candidate', { error: err.message, ...summary });
      });
    },
    flush() {
      ready = true;
      if (pending.length) {
        logger.info(`flushing ${pending.length} buffered remote ICE candidate(s)`);
      }
      while (pending.length) {
        const candidate = pending.shift();
        added += 1;
        peerConnection.addIceCandidate(candidate).catch((err) => {
          logger.warn('failed to add buffered candidate', {
            error: err.message, ...summarizeCandidate(candidate),
          });
        });
      }
    },
  };
}

/** Log every signaling-layer and ICE-layer state transition on a connection. */
function attachConnectionLogging(peerConnection, logger) {
  peerConnection.onsignalingstatechange = () =>
    logger.info(`signaling state -> ${peerConnection.signalingState}`);
  peerConnection.onicegatheringstatechange = () =>
    logger.info(`ICE gathering -> ${peerConnection.iceGatheringState}`);
  peerConnection.oniceconnectionstatechange = () =>
    logger.info(`ICE connection -> ${peerConnection.iceConnectionState}`);
  peerConnection.onicecandidateerror = (event) =>
    logger.warn('ICE candidate error', {
      url: event.url, code: event.errorCode, text: event.errorText,
    });
}

module.exports = {
  describeChannel,
  createSignalingClient,
  createCandidateQueue,
  attachConnectionLogging,
  Role,
};
