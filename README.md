# BR_exploreHD_DVR

BlueOS extension for a **Raspberry Pi 5** used as a **multi-camera DVR** (e.g. four [exploreHD](https://bluerobotics.com/store/sensors-cameras/cameras/deepwater-exploration-explorehd-usb-camera/) USB cameras). Video is recorded from **MAVLink Camera Manager (MCM)** **H.264 RTSP** endpoints into **power-loss-friendly MPEG-TS** segments (default **5 minutes**).

This extension **does not configure MCM**. You must define streams in BlueOS (Video Streams / MCM UI, port **6020**). If **no** H264 RTSP streams are present after boot, the UI shows an error and **Retry boot**. If **fewer than four** streams exist, recording runs for **all** streams returned by MCM and a warning is shown.

## Features

- **Auto-start after boot**: waits for CPU load to settle, zips prior session folders that lack a session zip, mounts USB storage when present, then starts one GStreamer pipeline per MCM stream.
- **USB storage**: records to `/mnt/usb/BR_exploreHD_DVR/...` when a removable drive is mounted and has **≥ 5 GB** free; otherwise uses `/app/recordings` on the SD card.
- **Web UI** (default port **6010**, next to MCM): Status (per-camera), Live (embedded MCM WebRTC dev page on port 6020), Recordings (multi-select days, bulk zip download / delete). Port **5777** is used by BlueOS `mavlink-server`; this extension avoids it by default.
- **Segmented `.ts`**: `splitmuxsink` + `mpegtsmux`; truncated segments remain playable (TS is self-synchronizing).

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
    "6010/tcp": {}
  },
  "HostConfig": {
    "Binds": [
      "/usr/blueos/extensions/br_explorehd_dvr:/app/recordings",
      "/dev:/dev"
    ],
    "ExtraHosts": [
      "host.docker.internal:host-gateway"
    ],
    "PortBindings": {
      "6010/tcp": [
        {
          "HostPort": ""
        }
      ]
    },
    "NetworkMode": "host",
    "Privileged": true
  }
}
```

Click **Create**. BlueOS will pull the image from Docker Hub and start the container. The extension appears in the sidebar once it's up (usually 10–30 s after the pull finishes). Web UI: **http://\<vehicle\>:6010/**.

What each piece does:
- `Binds` — persists recordings to `/usr/blueos/extensions/br_explorehd_dvr` on the host (so they survive extension reinstalls) and exposes `/dev` so USB storage can be mounted from inside the container.
- `NetworkMode: host` — required so the extension can reach MCM at `127.0.0.1:6020` and RTSP at `127.0.0.1:8554`.
- `Privileged: true` — required for mounting removable storage (`exfat-fuse`, `mount`, `util-linux`).
- `ExtraHosts: host.docker.internal:host-gateway` — lets the frontend iframe MCM's WebRTC UI via a stable hostname.

### Option B — Manual install from a `.tar` (air-gapped / offline)

Use this when the vehicle has no internet connection to pull from Docker Hub. Copy the tar built by this repo to the Pi, then:

```bash
docker load -i br_explorehd_dvr_linux_arm64_v1.0.23.tar
# Image tag: vshie/br_explorehd_dvr:1.0.23
```

Then register the extension in BlueOS using the same fields as Option A, but change:
- **Docker image**: `vshie/br_explorehd_dvr`
- **Docker tag**: `1.0.23`

(These match the image tag produced by `docker load`; Option A's `vshie/blueos-br_explorehd_dvr:main` is the Docker Hub published image and differs by name.)

## MCM prerequisites

1. Open **http://&lt;vehicle&gt;:6020/** and create one **H.264** stream per camera (RTSP endpoint will appear in MCM’s stream list).
2. Ensure the extension can `GET http://127.0.0.1:6020/streams` (default `MCM_BASE`).

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MCM_BASE` | `http://127.0.0.1:6020` | MCM REST base URL |
| `SEGMENT_SECONDS` | `300` | TS segment duration |
| `PORT` | `6010` | Flask listen port (override if needed) |
| `BOOT_MIN_SLEEP_S` | `20` | Minimum sleep before loadavg gate |
| `BOOT_LOADAVG_MAX` | `2.0` | 1m loadavg threshold |
| `MCM_MAX_WAIT_S` | `60` | Max wait polling `/streams` at boot |
| `EXTERNAL_STORAGE_DEVICE` | _(unset)_ | Optional explicit partition to mount at `/mnt/usb` (e.g. `/dev/nvme0n1p1`) if auto-detection does not pick your drive |
| `DVR_RTSP_PROTOCOLS` | `udp+tcp` | rtspsrc transport preference: `udp+tcp` (UDP with TCP fallback; default since 1.0.20), `tcp` (RTSP-over-TCP only — useful on lossy links), or `udp` (UDP only, no fallback). UDP avoids TCP head-of-line blocking when the MCM producer briefly glitches. |

## Recording layout

```
/app/recordings/YYYYMMDD/<session_uuid>/cam_<n>_<sanitized_name>/seg_00001.ts
```

When external storage is mounted at `/mnt/usb` with enough free space, recording uses `/mnt/usb/BR_exploreHD_DVR/` instead of `/app/recordings`. That includes **USB flash**, **USB‑bus M.2/NVMe enclosures** (often `/dev/sd*`), and **native NVMe** (`/dev/nvme*n*p*`) when it is not the OS disk. **exFAT** (or FAT32) is supported; the image includes `exfat-fuse`, and the extension tries generic `mount` then explicit `-t exfat` / `-t vfat`.

## API (short)

- `GET /status` — boot stage, errors, per-camera recorder status, telemetry, USB.
- `GET /streams` — normalized stream list used for recording.
- `POST /stop` / `POST /start` — stop or resume all recorders.
- `POST /cam/<index>/restart` — restart one pipeline.
- `POST /boot/retry` — re-run boot (e.g. after fixing MCM).
- `GET /recordings` — days + sessions + segment download URLs.
- `GET /download_day/<YYYYMMDD>` — zip one calendar day (`sd/` and `usb/` prefixes inside zip if both exist).
- `POST /download_days` — JSON `{"dates":["YYYYMMDD",...]}` → zip.
- `POST /recordings/delete` — JSON `{"dates":[...]}` (skips the calendar day of the active session).

## Live view

The **Live** tab iframes **`http://<hostname>:6020/webrtc`** (MCM WebRTC development UI). Pick the matching stream in that UI if your MCM build does not support deep-linking by stream ID.

## License

MIT — see [LICENSE](LICENSE).
