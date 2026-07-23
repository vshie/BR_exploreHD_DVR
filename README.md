# BR_exploreHD_DVR

BlueOS extension for a **Raspberry Pi 5** used as a **multi-camera cloud uplink** (e.g. four [exploreHD](https://bluerobotics.com/store/sensors-cameras/cameras/deepwater-exploration-explorehd-usb-camera/) USB cameras). Reads **MAVLink Camera Manager (MCM)** **H.264 RTSP** endpoints and pushes each one to a hardcoded RTMP cloud endpoint via `ffmpeg -c:v copy` (no re-encode). Also serves an in-browser **Live** view via MCM's WebRTC page.

**Cloud-only build.** Disk recording, USB storage, and downloads have been removed; the extension is a thin RTMP relay + Live viewer.

This extension **does not configure MCM**. You must define streams in BlueOS (Video Streams / MCM UI, port **6020**). If **no** H264 RTSP streams are present after boot, the UI shows an error and **Retry**. If **fewer than four** streams exist, the relay runs for **all** streams returned by MCM and a warning is shown.

## Features

- **Fast boot**: waits only for MCM to publish streams, then spawns one `ffmpeg -c:v copy -an -f flv` per RTSP source. No CPU-calm gate, no disk zip, no USB mount.
- **Cloud RTMP relay** (on by default, toggle in the **Cloud** tab): forwards each MCM RTSP stream to `rtmp://35.83.28.160/live/bom_cam0N`. Never re-encodes.
- **Live WebRTC view**: embeds MCM's WebRTC page for local monitoring (Quad + single).
- **Web UI** on port **4444**, next to MCM (which uses **6020**). Port **5777** is used by BlueOS `mavlink-server`; this extension avoids it by default.

## BlueOS install

### Option A — Manual install via the BlueOS UI (recommended)

In BlueOS, open **Extensions → Installed → +** (the plus icon, bottom right) to open the **Create Extension** dialog, then fill it in exactly as below:

| Field | Value |
|-------|-------|
| Extension Identifier | `br.dvr` |
| Extension Name | `BR_exploreHD_DVR` |
| Docker image | `vshie/blueos-br_explorehd_dvr` |
| Docker tag | `main` |

Paste this into the **Custom settings / Permissions** JSON editor:

```json
{
  "ExposedPorts": {
    "4444/tcp": {}
  },
  "HostConfig": {
    "Binds": [
      "/usr/blueos/extensions/br_explorehd_dvr:/app/recordings"
    ],
    "ExtraHosts": [
      "host.docker.internal:host-gateway"
    ],
    "PortBindings": {
      "4444/tcp": [
        {
          "HostPort": ""
        }
      ]
    },
    "NetworkMode": "host"
  }
}
```

Click **Create**. BlueOS will pull the image from Docker Hub and start the container. Web UI: **http://\<vehicle\>:4444/**.

What each piece does:
- `Binds` — persists the settings file (`.br_explorehd_dvr_settings.json`) to `/usr/blueos/extensions/br_explorehd_dvr` on the host so the cloud-relay toggle survives extension reinstalls.
- `NetworkMode: host` — required so the extension can reach MCM at `127.0.0.1:6020` and RTSP at `127.0.0.1:8554`.
- `ExtraHosts: host.docker.internal:host-gateway` — lets the frontend iframe MCM's WebRTC UI via a stable hostname.

## MCM prerequisites

1. Open **http://&lt;vehicle&gt;:6020/** and create one **H.264** stream per camera (RTSP endpoint will appear in MCM's stream list).
2. Ensure the extension can `GET http://127.0.0.1:6020/streams` (default `MCM_BASE`).

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MCM_BASE` | `http://127.0.0.1:6020` | MCM REST base URL |
| `DVR_RTSP_HOST` | `127.0.0.1` | Hostname substituted into MCM RTSP URLs for local ingest. MCM often advertises a stale LAN IP; loopback is correct under `NetworkMode: host`. |
| `PORT` | `4444` | Flask listen port (override if needed) |
| `MCM_POLL_INTERVAL_S` | `1` | MCM `/streams` poll interval during boot |
| `MCM_MAX_WAIT_S` | `60` | Max wait polling `/streams` at boot |

## API

- `GET /status` — boot stage, streams count, telemetry, cloud summary.
- `GET /streams` — normalized MCM stream list.
- `POST /live/ensure_streams` — idempotent kick to start MCM pipelines for WebRTC.
- `GET /settings` / `POST /settings` — read / update `cloud_relay_enabled`.
- `GET /cloud/status` — per-cam RTMP relay state, restart counts, errors.
- `POST /cloud/toggle` — body `{"enabled": true|false}` — flip the persisted toggle and start/stop the relay.
- `POST /boot/retry` — re-run MCM discovery + relay start (e.g. after fixing MCM).

## Cloud RTMP relay

Per cam, the extension spawns:

```
ffmpeg -rtsp_transport tcp \
       -stimeout 5000000 \
       -i <rtsp_url_from_mcm> \
       -c:v copy -an -f flv \
       rtmp://35.83.28.160/live/bom_cam0N
```

`-c:v copy` means **no re-encoding** — the relay is essentially free CPU-wise and just remuxes the existing H.264 elementary stream into FLV/RTMP. `-an` drops audio. `-stimeout 5000000` is a 5 s socket I/O timeout on the RTSP input, so a dead uplink causes ffmpeg to exit promptly instead of hanging on kernel timeouts. The destination RTMP server URL is intentionally hardcoded; on/off is the only operator-facing knob. The toggle persists in `.br_explorehd_dvr_settings.json` under the bind mount so it survives container/image rebuilds.

### Stream-key mapping (name-driven)

The per-camera RTMP **stream key** is derived from the **stream name you configure in BlueOS/MCM**: the number in the name selects the `bom_camNN` bucket (zero-padded to two digits). So a stream named `... 5` publishes to `bom_cam05`. This means a vessel whose cameras are named/numbered **5–8** publishes to `bom_cam05..bom_cam08`, while a vessel numbered **1–4** publishes to `bom_cam01..bom_cam04` — no per-deployment configuration, and no key collisions on a shared receiver.

- The **last** run of digits in the name is used (`exploreHD 5`, `cam5`, `1080p Front 5` all → `05`).
- A stream whose name has **no number** falls back to its list position (`0..3 → bom_cam01..bom_cam04`).
- If two streams resolve to the **same** number, both would fight over one RTMP publisher slot; the extension logs an error naming the offending streams so you can rename them in BlueOS.

**RTSP transport is TCP** (RTP-over-RTSP-interleaved). Every cloud-relay ffmpeg reads MCM's RTSP over loopback (`rtsp://127.0.0.1:8554/...`) at the same time as the browser's WebRTC Live view is reading the same streams — up to eight concurrent RTSP consumers on `127.0.0.1`. On loopback, TCP costs effectively nothing (no physical medium, no HOL blocking of a real link, memcpy-only cost) while UDP can drop RTP packets at the kernel receive buffer under that concurrency before anything ever leaves the vessel. TCP eliminates that class of on-box loss for the price of a slightly larger per-packet framing.

### Reconnect strategy

If ffmpeg exits (RTSP source disappeared, RTMP socket dropped, container restart), a per-cam watchdog respawns it:

- **Healthy run (≥ 30 s) before the exit** → respawn after **5 s** (the "Wi-Fi blip, come back fast" case).
- **Short-lived run (< 30 s)** → exponential backoff **5 → 10 → 20 → 40 → 60 s** (capped).
- Every wait has **0 – 2 s of uniform jitter** added on top so all four cams don't reconnect on the exact same tick after a common disturbance.

A separate, longer schedule activates only when the upstream RTMP server replies `Server error: Already publishing` — i.e. it's still holding our previous publisher slot. In that case the relay backs off **5 → 10 → 20 → 40 → 80 → 90 s** (capped) and the per-cam state in the UI shows `waiting for RTMP release`. Both schedules reset the moment a pipeline runs healthily for ≥ 30 s.

## Live view

The **Live** tab uses MCM's WebRTC page (`http://<hostname>:6020/webrtc`) via `mcm_webrtc_live.js`. Both quad and single-camera layouts are available.

## License

MIT — see [LICENSE](LICENSE).
