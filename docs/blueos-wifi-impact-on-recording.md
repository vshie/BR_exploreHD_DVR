# BlueOS WiFi management can silently starve camera recording

**Audience:** maintainers of BlueOS extensions that record RTSP/video or write
large volumes of data to disk. Relevant examples: `BR_exploreHD_DVR` (the
source of these measurements) and `blueos-doris`
(https://github.com/brianhBR/blueos-doris). Both ingest RTSP via MCM (MAVLink
Camera Manager) and persist MPEG‑TS segments on local or USB storage while
BlueOS simultaneously manages WiFi interfaces.

**Summary:** On a Raspberry Pi 5 running BlueOS, a misconfigured or out-of-range
WiFi station profile on the onboard Broadcom adapter (`brcmfmac`) puts
`wpa_supplicant` into a permanent association-retry loop. That loop produces
floods of `brcmf_set_channel: set chanspec 0x…, fail, reason -52` kernel
messages and tens of millions of SDIO interrupts over a session, which is
sufficient to starve GStreamer/ffmpeg RTSP ingest even though nothing in the
recording stack or in MCM is actually broken. The failure mode is
camouflaged: `/streams` still reports `running: true`, RTSP TCP sockets stay
established, file-growth watchdogs see no data, and the operator sees
"segments stalling" on perfectly healthy camera hardware.

---

## What we observed (BR_exploreHD_DVR, on a Pi 5 / BlueOS)

Four exploreHD cameras (USB UVC, not MIPI CSI) exposed by MCM 0.2.4 at
`rtsp://192.168.2.2:8554/…`. Our extension records each with
`gst-launch-1.0 rtspsrc … ! splitmuxsink muxer-factory=mpegtsmux` to
5-minute MPEG‑TS segments on internal SD.

Symptoms that kicked off the investigation:

- Recording segments rolled at ~30–170 s instead of 300 s.
- Watchdog logged `rtsp ingest stalled (no rchar growth for 30s), restarting
  pipeline` for all four cameras in near‑lockstep, producing a thrashing
  restart loop.
- MCM reported every stream as `running=true, error=null`, and TCP sessions
  on `:8554` stayed `ESTABLISHED` with `Recv-Q=0 / Send-Q=0` — connections
  alive, no data crossing them.
- A bare `gst-launch-1.0 rtspsrc protocols=tcp ! fakesink` issued from
  inside `blueos-core` (i.e. bypassing our extension entirely) reproduced
  the stall: **0 bytes over 10–20 s** on 1, 2, 3, and 4 concurrent consumers
  alike, including immediately after `docker restart blueos-core`.

For about an hour this looked conclusively like an MCM 0.2.4 bug in its
RTSP fanout. `POST /restart_streams` returns HTTP 500 "Missing argument"
for every request shape in 0.2.4, which reinforced the bad theory because
we couldn't kick MCM out of what looked like a wedged state.

### What turned out to be the actual cause

`dmesg -T | grep brcmfmac` showed this pattern repeating at a high rate:

```
brcmfmac: brcmf_set_channel: set chanspec 0x100c fail, reason -52
brcmfmac: brcmf_set_channel: set chanspec 0x100d fail, reason -52
brcmfmac: brcmf_set_channel: set chanspec 0x100e fail, reason -52
brcmfmac: brcmf_set_channel: set chanspec 0xd022 fail, reason -52
brcmfmac: brcmf_set_channel: set chanspec 0xd026 fail, reason -52
brcmfmac: brcmf_set_channel: set chanspec 0xd02a fail, reason -52
brcmfmac: brcmf_set_channel: set chanspec 0xd02e fail, reason -52
```

Context that makes this destructive on Pi 5 + BlueOS:

| Signal | Value we observed | Notes |
|---|---|---|
| `wlan0` state | `DOWN  NO-CARRIER` | station side never associated |
| `wpa_supplicant` | running on `wlan0`, retrying | one instance per boot |
| `iw reg get` | empty output | regulatory domain not set — every attempted chanspec refused with `-52` |
| `/proc/interrupts` `mmc1` | **13.1 M interrupts in 5 h 17 m** uptime (~700 IRQ/s average) | SDIO bus to the Broadcom chip |
| `top` during a flood burst | `50.0 sy` on one core | brcmfmac driver in kernel context |
| `brcmfmac` message rate in dmesg | >1 600 lines in a ~20 minute window of the session | bursty; quiet between retries |

`mmc1` is the SDIO controller that talks to the onboard Broadcom WiFi chip.
The driver doesn't give up — every scan iteration issues a fresh round of
`chanspec` changes, each one a round-trip over SDIO plus an IRQ pair. At
~700 IRQ/s sustained, plus bursts to several thousand per second during
active scan windows, the `brcmfmac` kernel threads pin a core and compete
for I/O scheduling slots with every other real-time consumer on the system.

### Proof it was the WiFi loop and not MCM

After `sudo killall wpa_supplicant && sudo ip link set wlan0 down`,
re-ran the same bare `gst-launch` RTSP test on the exact same MCM process:

```
4× TCP parallel  chain msgs / 10 s:   140  150  170  201
4× UDP parallel  chain msgs / 10 s:   147  156  183  201
```

All four cameras, both transports, streaming cleanly and concurrently.
The "MCM bug" evaporated as soon as the kernel wasn't fighting the WiFi
chip. No MCM restart was required; no changes to the recording pipeline
were required.

## Why this matters for any BlueOS extension that records video

The WiFi flood does not directly touch camera devices, the MCM process, or
our recorder's file descriptors. It starves them indirectly through three
mechanisms that any RTSP→disk extension is vulnerable to on identical
hardware:

1. **Kernel softirq / scheduling contention.** `brcmfmac` runs on the same
   CPU cores as MCM's GStreamer pipelines and your recorder's
   ffmpeg/gst‑launch processes. During a scan flood, preempted sockets
   don't get their `recvmsg()` woken promptly, so the RTSP client appears
   to stall while the kernel is busy talking to the WiFi chip.

2. **SDIO bus saturation.** On Pi 4 the onboard WiFi SDIO and the SD card
   controller share some plumbing and firmware state; on Pi 5 they sit on
   separate controllers (`mmc1` vs `mmc0`), but the block layer still
   arbitrates across them and per-core IRQ affinity can steer heavy IRQ
   load at the same core that's running the recorder thread. With
   hundreds of IRQ/s on `mmc1`, SD writes on `mmc0` pick up variable
   latency spikes that look exactly like an SD GC pause.

3. **CPU budget.** Four 1080p30 H.264 remux pipelines are already
   CPU-sensitive on a Pi 5. Adding a kernel-side draw of a half-core of
   sustained `sy` time plus several short 100%-one-core spikes per minute
   is enough to push RTSP demux below real-time on at least one stream.

The detection pattern is identical to the one the user hit and is unfun
to chase, because it looks like:

- MCM is broken (all streams `running=true`, no video flowing)
- SD card is stalling (writes stop, file growth stops)
- Recorder is bugged (watchdog-induced restart storm)

…when really the fault is 100 % at the WiFi management layer.

## Why this is especially relevant to Doris

Reading through `blueos-doris/extension/backend/src/doris/services/ip_camera_recorder.py`
and the repo's `NETWORKING_CHANGES.md`, three things stand out:

1. **Doris also records RTSP to segmented MPEG-TS.** It uses
   `ffmpeg -c copy -f segment` rather than GStreamer `splitmuxsink`, but
   the same socket path applies. When the kernel pauses `recvmsg()` on
   the RTSP TCP FD, ffmpeg will log the same "non-monotonic DTS" /
   "Connection timed out" style messages that a `gst-launch` stall
   produces, and ffmpeg's own `-reconnect`/`-rw_timeout` behavior will
   kick in — which cycles an RTSP TEARDOWN/SETUP against MCM and, in the
   MCM 0.2.4 state we saw, can wedge MCM's RTSP server across all
   streams rather than recovering just one.

2. **Doris carries two WiFi interfaces.** The bundled Realtek `88x2bu`
   USB adapter is the primary AP. The Pi's onboard `brcmfmac` is still
   present, and is still running `wpa_supplicant` whether or not Doris
   uses it, because BlueOS manages it independently of the extension.
   Doris can therefore inherit this failure mode from BlueOS even though
   Doris never asks the brcmfmac chip to do anything.

3. **Doris's `NETWORKING_CHANGES.md` already documents a class of
   WiFi-vs-extension interactions.** The note that `dnsmasq` fails to
   bind for 10–30 s after `configure_hotspot()` because `create_ap` is
   asynchronous, and the note that an AP watchdog was needed to catch
   dnsmasq dying after the interface comes up, describe the same
   underlying problem: BlueOS's WiFi management is not quiescent, and
   any extension running alongside it can be knocked around by kernel-
   level side effects. An extension that writes large segmented files
   to USB storage while a 88x2bu hostapd is reconfiguring + a brcmfmac
   wpa_supplicant is looping is inheriting a hostile scheduling
   environment that isn't visible from the extension's logs.

## Recommended mitigations (for Doris, BR_exploreHD_DVR, and similar)

Three layers. Pick what fits — they stack.

### Layer 1 — Operational / in the field

The fastest recovery once you see stalls is to check dmesg before
blaming the camera or MCM. If `brcmfmac` scan errors appear multiple
times per second, treat the extension's stream logs as unreliable
until WiFi is calmed:

```bash
dmesg -T | grep -c brcmfmac           # total count since boot
grep '^162:' /proc/interrupts         # mmc1 IRQ count (cumulative)
iw reg get                            # should return a country code, not empty
ip -br link show wlan0                # DOWN + NO-CARRIER + wpa_supplicant running = trouble
```

Remediations in order of preference:

1. Fix the station profile (enter credentials for a real reachable SSID,
   or remove the ghost SSID that `wpa_supplicant` is chasing).
2. Turn WiFi off entirely when not needed: `nmcli radio wifi off`, or
   `systemctl stop wpa_supplicant@wlan0 && ip link set wlan0 down`.
3. Ensure a regulatory domain is set (`iw reg set US`, or via crda /
   country=US in `wpa_supplicant.conf`). The empty reg domain is why
   every chanspec is rejected and why the loop never self-terminates.

### Layer 2 — Recorder-side tolerance

Regardless of the WiFi fix, a recording extension should tolerate
kernel-induced ingest pauses without making MCM's life worse.
`BR_exploreHD_DVR` v1.0.17 adopts the following; Doris could apply
equivalents in its ffmpeg wrapper:

- **Generous stall threshold on RTSP ingest** — we raised ours from
  30 s to **90 s**, and require the stall to be observed across **two
  consecutive watchdog polls** (~95 s of inactivity) before restarting
  the pipeline. Every TEARDOWN/SETUP we issue against MCM makes its
  state worse; restart only when clearly needed.
- **Tight batch starts, no long stagger.** MCM 0.2.4 serves the first
  RTSP client cleanly; clients that connect while the first is already
  `PLAYING` can starve. Open all per-camera pipelines with a small
  (~0.5 s) jitter rather than multi-second staggers.
- **Watch RTSP ingest, not file growth.** `/proc/<pid>/io rchar` tells
  you whether bytes are arriving from the socket, independently of
  whether splitmuxsink/ffmpeg is being backpressured by a slow SD
  fsync. Tying the watchdog to `rchar` stops SD latency from looking
  like an RTSP stall.
- **Large RAM queue between ingest and disk** — we run a 60 s /
  100 MB `queue leaky=no` between `rtph264depay ! h264parse` and
  `splitmuxsink`. It absorbs SD fsync bursts without applying
  backpressure upstream. ffmpeg equivalent: a generous
  `-rtbufsize`, plus an explicit muxer queue if using `-f segment`.
- **Don't call MCM `/restart_streams` on 0.2.4.** It returns 500 for
  every argument shape we tried, and even when it "succeeds" on other
  MCM versions, cycling MCM pipelines while RTSP fanout is already
  stressed can lock MCM across every stream. Make the call a no-op
  on this MCM and feature-flag it for future MCM builds that ship a
  working endpoint.

### Layer 3 — BlueOS / platform

These are outside the extension's control but worth raising with the
BlueOS team:

- The onboard `brcmfmac` chip should not loop forever without a valid
  regulatory domain. `iw reg get` returning empty on a shipped image is
  the bug that lets one bad SSID profile degrade every extension.
- `wpa_supplicant` on `wlan0` should back off aggressively (minute-scale,
  not second-scale) after N consecutive `-52` chanspec rejections, or
  mark the interface rfkill'd until the operator re-enables it.
- Ideally the BlueOS WiFi page's "reassociate" action should surface the
  reality — "we tried channels X, Y, Z and they're not allowed because
  your regulatory domain is empty" — rather than silently looping and
  letting the driver flood dmesg indefinitely.

## Appendix — reproducible diagnostic snippets

Observe the flood and its impact on disk/video:

```bash
# Pre-check
dmesg -T | grep brcmfmac | wc -l
grep '^162:' /proc/interrupts

# Is it happening right now?
A=$(dmesg | grep -c brcmfmac); sleep 10
B=$(dmesg | grep -c brcmfmac); echo "brcmfmac errors in last 10s: $((B-A))"

# mmc1 (SDIO->WiFi) IRQ/s sample
grep '^162:' /proc/interrupts | awk '{print $2}' > /tmp/i1; sleep 5
grep '^162:' /proc/interrupts | awk '{print $2}' > /tmp/i2
paste /tmp/i1 /tmp/i2 | awk '{ print "mmc1 IRQs/sec = " ($2-$1)/5 }'
```

Prove a stalled RTSP session is a kernel-side starvation rather than an
MCM fault, by streaming with no disk writes:

```bash
timeout 10 docker exec blueos-core gst-launch-1.0 -v \
  rtspsrc location=rtsp://192.168.2.2:8554/<stream> protocols=tcp latency=200 \
  ! rtph264depay ! h264parse ! fakesink sync=false silent=false 2>&1 \
  | grep -cE '\(fakesink0:sink\)'
```

Expected on a healthy system: >100 chain messages in 10 s per consumer,
scales to 4 concurrent consumers. Expected under a brcmfmac flood: 0.

Quick remediation for testing:

```bash
sudo killall wpa_supplicant
sudo ip link set wlan0 down
# re-run the gst-launch check above; if it goes from 0 -> ~150/10s,
# the WiFi loop is the cause.
```

---

Raised by: `BR_exploreHD_DVR` investigation, 2026-04-22.
Cross-referenced for discussion with: `blueos-doris`.
