# BR_exploreHD_DVR

BlueOS extension for a **Raspberry Pi 5** used as a **multi-camera DVR** (e.g. four [exploreHD](https://bluerobotics.com/store/sensors-cameras/cameras/deepwater-exploration-explorehd-usb-camera/) USB cameras). Video is recorded from **MAVLink Camera Manager (MCM)** **H.264 RTSP** endpoints into **power-loss-friendly MPEG-TS** segments (default **5 minutes**).

This extension **does not configure MCM**. You must define streams in BlueOS (Video Streams / MCM UI, port **6020**). If **no** H264 RTSP streams are present after boot, the UI shows an error and **Retry boot**. If **fewer than four** streams exist, recording runs for **all** streams returned by MCM and a warning is shown.

## Features

- **Auto-start after boot**: waits for CPU load to settle, zips prior session folders that lack a session zip, mounts USB storage when present, then starts one GStreamer pipeline per MCM stream.
- **USB storage**: records to `/mnt/usb/BR_exploreHD_DVR/...` when a removable drive is mounted and has **≥ 5 GB** free; otherwise uses `/app/recordings` on the SD card.
- **Web UI** (default port **6010**, next to MCM): Status (per-camera), Live (embedded MCM WebRTC dev page on port 6020), Recordings (multi-select days, bulk zip download / delete). Port **5777** is used by BlueOS `mavlink-server`; this extension avoids it by default.
- **Segmented `.ts`**: `splitmuxsink` + `mpegtsmux`; truncated segments remain playable (TS is self-synchronizing).

## BlueOS install

1. Build or install the extension image (Docker).
2. Bind host path: `/usr/blueos/extensions/br_explorehd_dvr` → `/app/recordings`.
3. Use **host network** and **privileged** so MCM (`127.0.0.1:6020`) and RTSP (`127.0.0.1:8554`) are reachable and USB drives can be mounted.

### Manual image from `.tar` (on the Pi or another Linux host)

```bash
docker load -i br_explorehd_dvr_linux_arm64_v1.0.2.tar
# Image tag: vshie/br_explorehd_dvr:1.0.2 (or your build tag)
```

Then register the extension in BlueOS pointing at that image, or run with the same `docker-compose` / labels as in this repo’s `Dockerfile`.

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

## Recording layout

```
/app/recordings/YYYYMMDD/<session_uuid>/cam_<n>_<sanitized_name>/seg_00001.ts
```

USB mirror uses `/mnt/usb/BR_exploreHD_DVR/` as the root instead of `/app/recordings` when eligible.

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
