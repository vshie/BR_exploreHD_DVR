# BR_exploreHD_DVR

BlueOS extension for a **Raspberry Pi 5** used as a **multi-camera DVR** (e.g. four [exploreHD](https://bluerobotics.com/store/sensors-cameras/cameras/deepwater-exploration-explorehd-usb-camera/) USB cameras). Video is recorded from **MAVLink Camera Manager (MCM)** **H.264 RTSP** endpoints into **power-loss-friendly MPEG-TS** segments (default **5 minutes**).

This extension **does not configure MCM**. You must define streams in BlueOS (Video Streams / MCM UI, port **6020**). If **no** H264 RTSP streams are present after boot, the UI shows an error and **Retry boot**. If **fewer than four** streams exist, recording runs for **all** streams returned by MCM and a warning is shown.

## Features

- **Auto-start after boot**: waits for CPU load to settle, zips prior session folders that lack a session zip, mounts USB storage when present, then starts one GStreamer pipeline per MCM stream.
- **USB storage**: records to `/mnt/usb/BR_exploreHD_DVR/...` when a removable drive is mounted and has **≥ 5 GB** free; otherwise uses `/app/recordings` on the SD card.
- **Web UI** (default port **4444**, next to MCM): Status (per-camera), Live (embedded MCM WebRTC dev page on port 6020), Recordings (multi-select days, bulk zip download / delete). Port **5777** is used by BlueOS `mavlink-server`; this extension avoids it by default.
- **Segmented `.ts`**: `splitmuxsink` + `mpegtsmux`; truncated segments remain playable (TS is self-synchronizing).

## Hardware setup (Raspberry Pi 5)

Do this once, before installing the extension.

### 1. Prepare the NVMe drive (recommended recording target)

The extension auto-detects an attached NVMe (or USB SSD) and prefers it over the SD card whenever it has ≥ 5 GB free. The drive must have a partition table and a filesystem; a brand-new disk ships raw and will not mount until you initialize it.

> **WARNING**: these commands erase the target disk. Verify the device path with `lsblk` first — `/dev/nvme0n1` is the M.2 slot on the Pi 5; do **not** run them against `/dev/mmcblk0` (your boot SD) or any drive that holds data you want to keep.

From a Pi shell (BlueOS host, **not** inside the container):

```bash
sudo wipefs -a /dev/nvme0n1
sudo parted -s /dev/nvme0n1 mklabel gpt mkpart primary ext4 0% 100%
sudo mkfs.ext4 -L BR_DVR /dev/nvme0n1p1
```

ext4 is preferred over exFAT/vfat for this workload: many small `.ts` segments with periodic `fsync`/finalize. exFAT and vfat are also supported (the image includes `exfat-fuse`), but vfat caps individual files at 4 GB.

The 30 s storage probe will mount the new partition at `/mnt/usb` automatically; no extension restart required. Verify with:

```bash
curl -s http://127.0.0.1:4444/status | python3 -m json.tool | grep -A6 '"usb"'
```

You should see `"mounted": true` and the device path. If auto-detection misses an unusual enclosure, set `EXTERNAL_STORAGE_DEVICE=/dev/nvme0n1p1` (or the matching `sdX1`) on the extension to force it.

#### Reference: NVMe vs SD card throughput on a Pi 5

Numbers from a Pi 5 with a Patriot M.2 P300 256 GB NVMe (PCIe link `Speed 5GT/s, Width x1` — the Pi 5's default Gen2 x1) compared to its boot SD card, both `dd`'d through ext4 from inside the extension container:

| Test | SD card (`/dev/mmcblk0p2`) | NVMe (`/dev/nvme0n1p1`) | NVMe / SD |
|------|----------------------------|--------------------------|-----------|
| Sequential write, 4 GiB, 1 MiB blocks, `fdatasync` | 73.5 MB/s | 396 MB/s | 5.4× |
| Sequential read, 4 GiB, 1 MiB blocks (cache-primed) | 180 MB/s | 800 MB/s | 4.4× |
| 64 KiB blocks, 1 GiB, `oflag=dsync` (fsync each block) | **8.5 MB/s** | **108 MB/s** | **12.7×** |
| Sequential write, 4 GiB, 4 MiB blocks, no sync | 97.9 MB/s | 504 MB/s | 5.1× |

The row that actually matters for this workload is the `oflag=dsync` one — that's the closest analogue to what `splitmuxsink` does on segment boundaries (write some payload, sync, finalize). Four exploreHD cameras at ~24 Mbps aggregate (~3 MB/s on disk) gives:

- **SD card**: ~24× headroom on bulk writes but only ~2.8× on the worst-case sync-bound path. Any competing latency source (WiFi, container churn, segment finalize on a sibling cam) erodes that and starts tripping the recorder's stall watchdog.
- **NVMe**: ~130× headroom on bulk, ~36× on the sync-bound path. Effectively unlimited margin.

If you observe segment stalls on SD-only systems, this gap is the underlying reason; moving recording to the NVMe is the durable fix. If you want even more NVMe headroom, adding `dtparam=pciex1_gen=3` to `/boot/firmware/config.txt` lifts the link from Gen2 x1 (~500 MB/s ceiling) to Gen3 x1 (~1000 MB/s); not necessary for four 1080p30 H.264 cameras.

`dd` invocation used (run inside the container at the target mount):

```bash
dd if=/dev/zero of=stest.bin bs=1M  count=4096 conv=fdatasync          # bulk write
dd if=stest.bin of=/dev/null bs=1M                                      # read back
dd if=/dev/zero of=stest.bin bs=64K count=16K conv=fdatasync oflag=dsync # sync-per-block
dd if=/dev/zero of=stest.bin bs=4M  count=1024                          # bulk no-sync
```

### 2. Camera power (4× exploreHD)

Each exploreHD draws roughly 1.5 A at peak. The Pi 5's combined USB rail cannot run four of them on bus power — symptoms are random USB resets, MCM streams dropping in/out, and `dmesg` `xhci`/`port reset` errors. With **four** exploreHD cameras connected:

- Pick **two** of the four cameras and **disconnect the 5 V (red) wire** from their USB‑A connectors so they are no longer powered from the Pi's USB bus.
- Splice those two 5 V leads to a separate, regulated **5 V supply** sized for ≥ 4 A combined (the cameras' grounds remain on the USB connector to share reference with the Pi). A common low-voltage drop is enough to cause intermittent stalls, so size the supply and wiring conservatively.
- Leave the data lines (D+/D−) and ground on the USB‑A connector untouched.
- The remaining two cameras stay fully USB-powered from the Pi.

This split keeps the Pi's USB controller within its current budget while preserving USB enumeration and per-camera v4l2 paths through MCM. Three or fewer cameras can run entirely on Pi USB power without modification.

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
      "/usr/blueos/extensions/br_explorehd_dvr:/app/recordings",
      "/dev:/dev"
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
    "NetworkMode": "host",
    "Privileged": true
  }
}
```

Click **Create**. BlueOS will pull the image from Docker Hub and start the container. The extension appears in the sidebar once it's up (usually 10–30 s after the pull finishes). Web UI: **http://\<vehicle\>:4444/**.

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
| `PORT` | `4444` | Flask listen port (override if needed) |
| `BOOT_MIN_SLEEP_S` | `20` | Minimum sleep before loadavg gate |
| `BOOT_LOADAVG_MAX` | `2.0` | 1m loadavg threshold |
| `MCM_MAX_WAIT_S` | `60` | Max wait polling `/streams` at boot |
| `EXTERNAL_STORAGE_DEVICE` | _(unset)_ | Optional explicit partition to mount at `/mnt/usb` (e.g. `/dev/nvme0n1p1`) if auto-detection does not pick your drive |
| `DVR_RTSP_PROTOCOLS` | `udp+tcp` | rtspsrc transport preference: `udp+tcp` (UDP with TCP fallback; default since 1.0.20), `tcp` (RTSP-over-TCP only — useful on lossy links), or `udp` (UDP only, no fallback). UDP avoids TCP head-of-line blocking when the MCM producer briefly glitches. |
| `NEURALX_ENDPOINT` | `https://vv4ki4fa6b.execute-api.us-west-2.amazonaws.com/test-upload-url` | Default URL the NeuralX uploader presigns through. Used on first boot only — once a value is persisted via the UI (`POST /neuralx/settings`), the stored value wins. |

## Recording layout

```
/app/recordings/YYYYMMDD/<session_uuid>/cam_<n>_<sanitized_name>/YYYYMMDD_HHMMSS_cam<n>.ts
```

The day directory and segment filenames are written in the **operator's browser-local time** (the UI reports its TZ to the extension, and the offset is persisted so auto-record-on-boot still produces correctly-stamped files before any client has connected). While a segment is actively being written it shows as `seg_00007.ts` under the splitmuxsink template; once `splitmuxsink` rolls to the next segment the closed file is renamed to `YYYYMMDD_HHMMSS_cam<n>.ts` within ~5 s. The `_cam<n>` suffix is important: four cams rolling on the same 5-minute boundary often finalize within the same wall-clock second, and the suffix is what keeps their filenames unique on disk and inside per-`camera_id` buckets on the NeuralX server. The active segment is renamed when the recorder stops or the pipeline is torn down (stall, disk-full, manual stop). Recordings produced by 1.0.29 and earlier (`YYYYMMDD_HHMMSS.ts`, no cam suffix) are still recognized by the auto-download cursor and the NeuralX uploader for backward compatibility.

When external storage is mounted at `/mnt/usb` with enough free space, recording uses `/mnt/usb/BR_exploreHD_DVR/` instead of `/app/recordings`. That includes **USB flash**, **USB‑bus M.2/NVMe enclosures** (often `/dev/sd*`), and **native NVMe** (`/dev/nvme*n*p*`) when it is not the OS disk. **exFAT** (or FAT32) is supported; the image includes `exfat-fuse`, and the extension tries generic `mount` then explicit `-t exfat` / `-t vfat`.

## API (short)

- `GET /status` — boot stage, errors, per-camera recorder status, telemetry, USB.
- `GET /streams` — normalized stream list used for recording.
- `POST /stop` / `POST /start` — stop or resume all recorders.
- `POST /cam/<index>/restart` — restart one pipeline.
- `POST /boot/retry` — re-run boot (e.g. after fixing MCM).
- `GET /recordings` — days + sessions + segment download URLs.
- `GET /download_day/<YYYYMMDD>` — zip one calendar day (`sd/` and `usb/` prefixes inside zip if both exist).
- `GET /download_session/<YYYYMMDD>/<sessionId>` — zip one session. Serves the pre-built `<sessionId>.zip` directly when present; otherwise stream-zips the session's `.ts` segments.
- `POST /download_days` — JSON `{"dates":["YYYYMMDD",...]}` → zip.
- `POST /recordings/delete` — JSON `{"dates":[...]}` (skips the calendar day of the active session).
- `POST /tz` — JSON `{"tz_offset_minutes": <signed int east of UTC>, "tz_name": "<IANA tz>"}` to inform the extension of the operator's browser-local timezone. The Web UI sends this on load and on visibility changes; only needed externally if you drive the API without the bundled UI.

## NeuralX upload (optional)

A continuous background uploader can ship each closed MPEG-TS segment to the [NeuralX test endpoint](https://vv4ki4fa6b.execute-api.us-west-2.amazonaws.com/test-upload-url) as it finalizes. **Disabled by default** — the documented endpoint is a public, unauthenticated test bucket with 7-day file retention, so this is opt-in per node.

Open the **NeuralX** tab in the extension UI to configure:

| Setting | Notes |
|---------|-------|
| Node ID | A short token (matches `[A-Za-z0-9._-]{1,40}`) that's prepended to every uploaded filename. Each Pi must use a different Node ID so two Pis sharing the same `camera_id` don't overwrite each other on the server. |
| Endpoint | Pre-filled with the documented NeuralX endpoint. Override per install if you have a private or staging URL. |
| Camera mapping | Per-cam dropdown that maps cam index 0..3 onto NeuralX `camera_id` 01..04. Defaults to `cam0→01, cam1→02, cam2→03, cam3→04`. Values must be unique within the node. |
| Workers | 1..4 concurrent uploads. Default `1` — fine for the four-camera ~24 Mbps aggregate over typical home internet. |

**Local cleanup policy (hardcoded, no UI knob).** Once a segment is marked `done` in the NeuralX state file, the uploader deletes the local copy when **either**:

- the file is older than **3 days** (mtime), or
- free space on the volume holding the file is below **50 GB**.

Both conditions are re-evaluated after each successful upload **and** by a periodic sweep every scan tick (~10 s), oldest-recording-first. Files that haven't uploaded successfully are **never** touched, so the rule cannot lose data the test bucket hasn't yet acknowledged.

The protocol mirrors the [NeuralX Raspberry Pi integration guide](https://vv4ki4fa6b.execute-api.us-west-2.amazonaws.com/test-upload-url) verbatim:

1. `GET {endpoint}?camera_id=<01..04>&filename=<node_id>_<basename>` → JSON `{ "upload_url": "..." }`.
2. `PUT <upload_url>` with the raw `.ts` bytes (AWS signature is embedded in the URL — no auth header from our side).

Per-file state (status, attempts, last error, Mbps, bytes) is persisted in `<recordings>/.br_explorehd_dvr_neuralx_state.json` and survives container restarts. The scanner walks every closed `YYYYMMDD_HHMMSS_cam<n>.ts` (and the legacy 1.0.29 `YYYYMMDD_HHMMSS.ts`) under both SD and USB roots, so when the uploader is enabled or the network recovers after an outage it will backfill anything that's still on disk and not yet marked as `done`.

> **Caveat — automatic cleanup + session zips.** When the cleanup sweep fires, the per-session `<session_id>.zip` archives built at boot by `zip_unfinished_sessions` may be partial (or absent) for sessions whose segments shipped before the zip pass ran. That's acceptable for the test-bucket workflow but worth knowing if you also rely on the per-session zip downloads in the Recordings tab.

Programmatic access:

- `GET /neuralx/status` — full payload: settings, queue counts, totals, recent uploads, and `delete_policy` block (`free_mb_threshold`, `max_age_days`, last sweep counters).
- `POST /neuralx/settings` — JSON body with any subset of `enabled`, `node_id`, `endpoint`, `cam_map`, `max_concurrent`. (The legacy `delete_below_free_mb` field is silently ignored — the cleanup policy is now hardcoded.) Validates camera_id whitelist + uniqueness and the `node_id` token regex.
- `POST /neuralx/retry` — reset the retry timer on every failed entry so the next scan re-enqueues it.

## Live view

The **Live** tab iframes **`http://<hostname>:6020/webrtc`** (MCM WebRTC development UI). Pick the matching stream in that UI if your MCM build does not support deep-linking by stream ID.

## License

MIT — see [LICENSE](LICENSE).
