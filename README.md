# KVS WebRTC two-way media

Two Node.js scripts that talk to each other over an Amazon Kinesis Video Streams
WebRTC signaling channel:

| Script | Role | Sends | Receives |
| --- | --- | --- | --- |
| `src/master.js` | MASTER (the panel) | video + audio | audio from each viewer |
| `src/viewer.js` | VIEWER (the operator) | audio (talk-back) | video + audio from master |

KVS is used for **signaling + STUN/TURN**; the media itself flows peer-to-peer
(or via the KVS TURN relay). Two-way audio is only possible over WebRTC — the
`PutMedia` ingestion API is one-way, so that path is not used here.

## Prerequisites

- Node.js 18+
- `ffmpeg` and `ffplay` on `PATH` (`sudo apt install ffmpeg`)
- Build toolchain for the `@roamhq/wrtc` prebuild (usually not needed — it ships
  binaries for linux x64/arm64)
- AWS credentials with `kinesisvideo:DescribeSignalingChannel`,
  `GetSignalingChannelEndpoint`, `GetIceServerConfig`, `ConnectAsMaster`,
  `ConnectAsViewer`

## Setup

```bash
cd kvs-webrtc
npm install
cp .env.example .env      # then edit it

# one-time, if the channel doesn't exist yet
aws kinesisvideo create-signaling-channel \
  --channel-name v6-access-panel \
  --single-master-configuration MessageTtlSeconds=60 \
  --region us-east-1
```

## Run

Terminal 1 — the panel:

```bash
npm run master
```

Terminal 2 — the operator:

```bash
npm run viewer
```

With the defaults you get a test pattern and a 440 Hz tone in both directions,
which is enough to confirm the whole path works end to end. Point
`VIDEO_INPUT=/dev/video0` and `AUDIO_INPUT=alsa:default` at real hardware once
that passes.

Record instead of playing back:

```bash
AUDIO_OUT=file VIDEO_OUT=file OUT_DIR=./recordings npm run viewer
```

## Monitor window

Both masters (`npm run master` and `npm run ingest`) serve a local dashboard so
you can see what is actually being published without reading log scrollback:

```bash
npm run master          # prints: open http://127.0.0.1:8088
```

Open <http://127.0.0.1:8088>. It shows, live:

- **Published video** — a JPEG thumbnail of the exact frames going out, taken
  from the same raw frames the encoder is fed (ffmpeg on the `master.js` path,
  a GStreamer `tee` branch before `x264enc` on the ingest path). If this moves,
  video is being published; if it is frozen, it is not.
- **Messages** — every signaling message in and out, with direction: SDP offers
  and answers (media sections summarised, full SDP behind a disclosure) and ICE
  candidates (type/protocol/address). Filter to signaling only, or to the log.
- **Peers** — one row per connected peer, whether it is a human viewer or the
  AWS storage session (`aws-storage`), its connection and ICE state, uptime.
- **RTP stats** — tx/rx kbps, fps, packet counts, loss, jitter and the winning
  ICE candidate pair, per peer.
- **Talk-back meter** — RMS level of the audio a viewer is sending back, so you
  can see the microphone working without listening to it.

It is read-only: no button on the page can change the session. Two extra HTTP
endpoints exist for scripting: `GET /state` (JSON snapshot) and
`GET /preview.jpg` (latest published frame).

| Variable | Default | Meaning |
| --- | --- | --- |
| `MONITOR` | `true` | `false` disables the dashboard entirely |
| `MONITOR_PORT` | `8088` | Port to listen on (a busy port logs a warning and disables it) |
| `MONITOR_HOST` | `127.0.0.1` | Bind address. `0.0.0.0` publishes your live video to the LAN |
| `MONITOR_PREVIEW_FPS` | `4` | Thumbnail rate. Does not affect the published stream |

The preview never back-pressures media: frames are dropped for the thumbnail
encoder and for slow browsers before they can slow the encoder down.

## Logging

Every stage logs. Set the verbosity with `LOG_LEVEL`:

| Level | What you get |
| --- | --- |
| `error` / `warn` | only failures |
| `info` (default) | numbered `STEP` milestones, SDP summaries, track events, state transitions, RTP stats every 5 s |
| `debug` | + every ICE candidate (type/protocol/address), ffmpeg command lines and stderr, frame/PCM throughput, audio queue depth and underruns |
| `trace` | + full SDP bodies |

```bash
LOG_LEVEL=debug npm run viewer
LOG_TIMESTAMPS=false NO_COLOR=1 LOG_LEVEL=trace npm run master 2>&1 | tee viewer.log
```

Each line is `wall-clock +elapsed LEVEL [component] message key=value …` — the
`key=value` tail is deliberately greppable:

```
10:11:58.177 +   0.00s INFO  [kvs] STEP 03 ▸ GetSignalingChannelEndpoint for role MASTER protocols=WSS,HTTPS
10:11:58.179 +   0.35s INFO  [master:viewer-1a2b] offer: 2 media section(s) type=offer bytes=3184
10:11:58.179 +   0.35s INFO  [master:viewer-1a2b]   m=audio mid=0 direction=sendrecv codecs=opus
10:11:58.179 +   0.35s INFO  [master:viewer-1a2b]   m=video mid=1 direction=recvonly codecs=VP8,rtx
10:11:59.402 +   1.57s DEBUG [master:viewer-1a2b] local ICE candidate -> viewer type=srflx protocol=udp address=…
10:12:03.410 +   5.58s INFO  [master:viewer-1a2b] selected candidate pair local=srflx/udp remote=host/udp rttMs=24
10:12:03.410 +   5.58s INFO  [master:viewer-1a2b] tx video kbps=812 packets=1204 fps=30 size=640x480
10:12:03.410 +   5.58s INFO  [master:viewer-1a2b] rx audio kbps=34 packets=498 jitterMs=3
```

The master tags each peer with the viewer's client id (`[master:viewer-1a2b]`),
so concurrent sessions stay separable in one log.

Reading the output when something is wrong:

- **No `STEP ▸ SDP OFFER received`** on the master — the viewer never reached the
  channel: wrong channel name, wrong region, or missing IAM permission.
- **Offer/answer exchanged but connection stays `connecting`** — look at the ICE
  candidate lines. Only `host` candidates means STUN/TURN never answered; set
  `KVS_FORCE_TURN=true`.
- **`connected` but no media** — `tx`/`rx` lines show `kbps=0`, or the `rx` lines
  never appear at all. `track muted` warnings mean packets stopped arriving.
- **Choppy audio** — watch `outbound audio pipeline underruns=` and
  `droppedChunks=`; a non-zero `droppedChunks` means the input is producing
  faster than real time (raise `AUDIO_MAX_BUFFER_MS`).

## How it fits together

```
master.js                                   viewer.js
  ffmpeg -> RTCVideoSource ─┐                 ┌─ RTCVideoSink -> ffplay/mp4
  ffmpeg -> RTCAudioSource ─┼── SRTP ─────────┼─ RTCAudioSink -> ffplay/wav
  ffplay <- RTCAudioSink  ──┘                 └─ RTCAudioSource <- ffmpeg (mic)
        │                                             │
        └──── KVS signaling (WSS, SigV4) ─────────────┘
              offer / answer / trickle ICE
```

- `src/kvs.js` — channel lookup, endpoint discovery, ICE server config,
  `SignalingClient` construction, and a queue for ICE candidates that arrive
  before the remote description is set.
- `src/log.js` — leveled logger, SDP summariser, ICE candidate parser, and
  rate-limited counters for the per-frame paths.
- `src/stats.js` — polls `getStats()` and prints the selected candidate pair,
  bitrates, fps, jitter and packet loss.
- `src/media.js` — ffmpeg-backed sources and sinks. Outbound audio is paced by a
  drift-corrected 10 ms timer (wrtc requires exactly 10 ms per `onData` call) and
  the buffer is capped so latency can't creep up.
- The master answers offers, so it handles multiple viewers concurrently; all of
  them share one video and one audio ffmpeg process.

## Ingestion + multiviewer (`ingest-master.js` / `ingest-viewer.js`)

The second pair archives to a Kinesis Video Stream **and** serves live viewers.

| Script | Role | Sends | Receives |
| --- | --- | --- | --- |
| `src/ingest-master.js` | MASTER | H.264 video + Opus audio, to storage and to viewers | viewer talk-back audio |
| `src/ingest-viewer.js` | VIEWER | nothing | H.264 video + Opus audio from AWS |

### The topology is not what you would guess

Without ingestion, viewers peer with the master directly and the master fans
out. **With ingestion enabled, they do not.** The master streams into the
storage session, each viewer calls `JoinStorageSessionAsViewer`, and **AWS**
performs the fan-out:

```
                    JoinStorageSession
  ingest-master ───────────────────────→ AWS ──→ Kinesis Video Stream (archive)
                                          │
                                          ├──→ ingest-viewer  (JoinStorageSessionAsViewer)
                                          └──→ ingest-viewer  ...
```

AWS sends the offer in both directions; both scripts only ever answer.

**Enabling media storage is a channel-wide mode switch.** While
`MediaStorageConfiguration` is ENABLED, a plain `viewer.js` offer is never
delivered to the master at all — this is a property of the channel, not of
whether you happen to call `JoinStorageSession`. Isolated by flipping only that
setting, everything else identical:

| `MediaStorageConfiguration` | `master.js` receives a `viewer.js` offer? |
| --- | --- |
| DISABLED | yes — connected, `rx audio kbps=32` |
| ENABLED | no — offer sent, never arrives |

So a channel is either a peer-to-peer intercom **or** an ingestion channel. It
cannot be both, and `ingest-viewer.js` exists because of it. Keep a separate
channel for the low-latency path.

### Does the master receive audio *from* AWS? Yes.

AWS's storage offer declares audio `sendrecv`, and it means it: a viewer's
talk-back is relayed through the storage session and arrives at the master.

An earlier round of testing here concluded the opposite, recording
`INBOUND PAD appeared: 0` over a 70 s session. That measurement was taken while
two separate faults were in play — the master defaulted to draining the storage
peer's audio unheard, and the inbound pad was classified on caps read before
they were negotiated, so a real Opus pad was filed as non-audio. With both
fixed, talk-back is audible. Treat the old "strictly one-way" claim as retired.

What it is *not* is fast. The relay goes viewer -> AWS -> master, and that hop
is not tunable from here. What is tunable is the local playback path, which
used to add ~200 ms of its own on top:

| Term | Where | Default | Now |
| --- | --- | --- | --- |
| jitter buffer | `gst_peer.py` webrtcbin `latency` | 40 ms | 40 ms |
| sink ring buffer | `pulsesink buffer-time` | 200 ms | `TALKBACK_SINK_MS`, 40 ms |
| decoded backlog | queue before the sink | unbounded | `TALKBACK_QUEUE_MS`, 60 ms |

The queue is the one that matters over a long session. The sink runs
`sync=false`, so nothing downstream ever drops a late buffer; without a cap, a
burst of late packets is played out in full and the delay it introduced stays
for the rest of the call. `leaky=downstream` drops the oldest instead, and
`opusdec plc=true` conceals the gap.

Measured on the sink alone, `buffer-time` 200 ms -> 40 ms took reported latency
from 153.2 ms to 73.2 ms.

### Why GStreamer and not wrtc

Ingestion requires **H.264**, and `@roamhq/wrtc` supports neither sending nor
receiving it:

```
wrtc send:    video/VP8 video/rtx video/VP9 video/AV1 …     (no H.264)
wrtc receive: video/VP8 video/rtx video/VP9 video/AV1 …     (no H.264)
```

So this path uses GStreamer's `webrtcbin` via a Python helper
(`gst_peer.py`, `gst_viewer.py`), with Node keeping AWS auth, discovery,
`JoinStorageSession*` and signaling. `master.js` / `viewer.js` are unchanged and
still use wrtc with VP8.

The master captures and encodes **once**, then a `tee` fans the encoded frames
out per peer. A camera can only be opened once, and encoding once for ten
viewers costs the same as for one. Only RTP payloading is per-peer, because
each peer negotiates its own payload type (AWS demands 126 for H.264).

### One-time setup

```bash
aws kinesisvideo create-stream --stream-name sabastian-test-store \
  --data-retention-in-hours 24 --media-type video/h264 \
  --profile kvs --region us-east-1

CH=$(aws kinesisvideo describe-signaling-channel --channel-name sabastian-test \
  --profile kvs --region us-east-1 --query ChannelInfo.ChannelARN --output text)
ST=$(aws kinesisvideo describe-stream --stream-name sabastian-test-store \
  --profile kvs --region us-east-1 --query StreamInfo.StreamARN --output text)

aws kinesisvideo update-media-storage-configuration --channel-arn "$CH" \
  --media-storage-configuration "Status=ENABLED,StreamARN=$ST" \
  --profile kvs --region us-east-1
```

Until that runs, `GetSignalingChannelEndpoint` refuses the `WEBRTC` protocol
with `MediaStorageConfiguration is required` — which is exactly the error
`kvs.js` catches and turns into the fix-it command.

Requires `gstreamer1.0-plugins-{base,good,bad,ugly}`, `gstreamer1.0-nice`,
`gstreamer1.0-libav` and `python3-gi`.

### Run

```bash
npm run ingest                                    # archives + serves viewers
VIDEO_OUT=file AUDIO_OUT=file npm run ingest-viewer   # one viewer, recorded
```

Confirm fragments are landing:

```bash
EP=$(aws kinesisvideo get-data-endpoint --stream-name sabastian-test-store \
  --api-name LIST_FRAGMENTS --profile kvs --region us-east-1 \
  --query DataEndpoint --output text)
aws kinesis-video-archived-media list-fragments \
  --stream-name sabastian-test-store --endpoint-url "$EP" \
  --profile kvs --region us-east-1
```

### Interop notes, learned the hard way

- **`a=bundle-only` with port 0.** AWS marks its video m-line `m=video 0` plus
  `a=bundle-only` (RFC 8843: "usable only inside the BUNDLE group"). webrtcbin
  reads the 0 as a rejection and answers `a=inactive`, so no video is ever
  archived. Both helpers rewrite the port to 9 and drop the attribute; safe,
  because everything is bundled onto one transport anyway.
- **Payload types must match per peer.** AWS wants H.264 on PT 126 and Opus on
  111. A shared RTP tee would pin one PT for everybody, so the tee carries
  encoded frames and each peer gets its own payloader.
- **m-line order matters.** webrtcbin assigns sink pads to m-lines in request
  order, so sources are linked only after the offer is parsed. Link video first
  against an audio-first offer and the video lands on the audio m-line and goes
  `inactive`.
- **`Gst.Promise` needs `wait()`.** Calling `get_reply()` inside a change func
  without `promise.wait()` first can return a reply whose `answer` is `None`,
  which then crashes `set-local-description`.
- **Recordings need fragmented MP4.** Plain `mp4mux` writes its index only at
  EOS, leaving a 0-byte file when the process is killed; `fragment-duration`
  makes it write incrementally.
- **Viewers receive buffered content.** A 25 s viewer session yielded 132 s of
  audio: the storage session replays what is already in the stream, so this is
  archive playback, not a live low-latency feed. Use `master.js`/`viewer.js`
  when you need sub-second latency.

### Verified end to end

Master archiving with two concurrent viewers:

```
[ingest:storage] ARCHIVING — media now flowing into the Kinesis Video Stream
[sview] LIVE — receiving the archived stream via AWS fan-out     (×2 viewers)
```

- KVS fragments: 2 × ~2 MB, ~10 s each
- Both viewers recorded 1280×720 H.264 (~27 s, 30 fps) and 48 kHz Opus→WAV
- Decoded audio measured at **440 Hz**, peak 26372 — the master's test tone
- Extracted video frame shows the SMPTE bars the master generated

## Notes and limits

- **Credentials are resolved once at startup.** For sessions longer than the
  lifetime of temporary credentials (SSO, assumed roles), restart the process or
  swap in a refreshing `requestSigner`.
- **Resolution changes:** WebRTC downscales under congestion. The raw video pipe
  can't signal that, so `VideoOutput` restarts ffmpeg and starts a new `.mp4`
  segment whenever the incoming frame size changes.
- **Recorded video timing** assumes `VIDEO_FPS`; frames arriving at a different
  rate will play back slightly fast or slow. Live playback (`VIDEO_OUT=play`) is
  unaffected.
- **No echo cancellation.** If you run both scripts on one machine with real
  speakers and mics, use headphones or `AUDIO_OUT=file`.
- **Picking audio devices.** `AUDIO_INPUT` takes `alsa:<device>` or
  `pulse:<node.name>`; `AUDIO_OUT_DEVICE` pins playback to one sink (unset =
  system default). Enumerate them with `wpctl status`, then
  `wpctl inspect <id> | grep node.name`. On a PipeWire system, direct
  `hw:N,M` access usually fails with an I/O error because the server holds the
  device — go through `default` or `pulse:` instead.
- **Inbound sample rate can change.** wrtc's audio sink may start at 16 kHz and
  switch to 48 kHz once Opus settles; `AudioOutput` restarts its ffmpeg/ffplay
  child when that happens, which shows up as two "playing remote audio" lines.
- If the connection reaches `failed`, set `KVS_FORCE_TURN=true` to pin media to
  the KVS relay.
