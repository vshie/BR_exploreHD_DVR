"""
BR_exploreHD_DVR — BlueOS extension: record MCM H264 RTSP streams to segmented MPEG-TS.
"""

from __future__ import annotations

import io
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, request, send_file

import usb_storage
from boot_manager import SEGMENT_SECONDS_DEFAULT, run_boot_sequence
from mcm_client import DEFAULT_MCM_BASE, fetch_streams_raw, kick_streams, list_h264_rtsp_streams
from recorder import RecorderManager
from settings_store import (
    browser_local_datetime,
    get_browser_tz_offset_minutes,
    load_settings,
    save_settings,
)
from system_telemetry import get_all_telemetry, get_disk_free_mb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

VERSION = "1.0.30"

RECORDINGS_LOCAL = "/app/recordings"
# Minimum free space required to start a recording session on internal SD.
# Goal: keep enough headroom for a full 4-cam session and avoid filling
# rootfs. External media does NOT need this gate (it's dedicated storage).
INTERNAL_SD_MIN_FREE_MB = 5 * 1024
MCM_BASE = os.environ.get("MCM_BASE", DEFAULT_MCM_BASE).rstrip("/")

_boot_lock = threading.Lock()
_state_lock = threading.Lock()

boot_stage = "starting"
boot_error: Optional[str] = None
recording_base: str = RECORDINGS_LOCAL
session_root: Optional[str] = None
current_session_id: Optional[str] = None
manager: Optional[RecorderManager] = None
streams_snapshot: List[Dict[str, Any]] = []
stopped_by_user = False
_disk_stopped = False


def _storage_roots() -> List[str]:
    roots = [RECORDINGS_LOCAL]
    if usb_storage.is_mounted():
        roots.append(os.path.join(usb_storage.USB_MOUNT_POINT, usb_storage.DVR_DIR))
    return roots


def _active_session_date() -> Optional[str]:
    if not session_root:
        return None
    # session_root = .../YYYYMMDD/session_id
    return os.path.basename(os.path.dirname(session_root))


def _disk_free_for_session() -> Optional[float]:
    path = session_root or recording_base
    return get_disk_free_mb(os.path.dirname(path))


def _on_disk_critical():
    global manager, _disk_stopped
    _disk_stopped = True
    if manager:
        try:
            manager.stop_all()
        except Exception:
            pass


def _boot_worker():
    global boot_stage, boot_error, recording_base, session_root, current_session_id
    global manager, streams_snapshot, stopped_by_user, _disk_stopped
    with _boot_lock:
        try:
            stopped_by_user = False
            _disk_stopped = False
            rb, sr, sid, streams, err, stage = run_boot_sequence(MCM_BASE)
            with _state_lock:
                boot_stage = stage
                recording_base = rb
                session_root = sr
                current_session_id = sid
                streams_snapshot = list(streams)
                boot_error = err
            if err or not sr:
                logger.error(err or "No session root")
                with _state_lock:
                    boot_stage = "mcm_error"
                    manager = None
                return
            segment_ns = int(os.environ.get("SEGMENT_SECONDS", str(SEGMENT_SECONDS_DEFAULT))) * 1_000_000_000
            internal_sd = (rb == RECORDINGS_LOCAL)
            mgr = RecorderManager(
                sr,
                streams,
                segment_ns,
                _disk_free_for_session,
                on_disk_critical=_on_disk_critical,
                is_internal_sd=internal_sd,
            )
            auto_rec = bool(load_settings().get("auto_record_on_boot", True))
            # 5 GB free precondition for recording to internal SD. External media is
            # assumed dedicated and is gated only by the general disk-critical watchdog.
            sd_blocked = False
            if auto_rec and internal_sd:
                free_mb = _disk_free_for_session()
                if free_mb is not None and free_mb < INTERNAL_SD_MIN_FREE_MB:
                    sd_blocked = True
                    logger.error(
                        f"Internal SD has only {free_mb:.0f} MB free; need "
                        f"{INTERNAL_SD_MIN_FREE_MB} MB to auto-start. Staying in standby. "
                        f"Free space, switch to external media, or press Start to override."
                    )
            with _state_lock:
                manager = mgr
                if sd_blocked:
                    boot_stage = "sd_low"
                    boot_error = (
                        f"Internal SD has < {INTERNAL_SD_MIN_FREE_MB // 1024} GB free. "
                        f"Attach external media or free space, then press Start."
                    )
                else:
                    boot_stage = "recording" if auto_rec else "standby"
            if auto_rec and not sd_blocked:
                mgr.start_all()
            elif sd_blocked:
                logger.info("SD gate blocked auto-record; recorders created but not started")
            else:
                logger.info("auto_record_on_boot is false; recorders created but not started (standby)")
        except Exception as e:
            logger.exception("Boot worker failed")
            with _state_lock:
                boot_error = str(e)
                boot_stage = "error"
                manager = None


def _retry_mcm_list() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        streams = list_h264_rtsp_streams(base=MCM_BASE)
        if not streams:
            return [], "No H264 RTSP streams available from MCM."
        return streams, None
    except Exception as e:
        return [], f"MCM unreachable: {e}"


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/favicon.ico")
def favicon_ico():
    """Some browsers / shells request /favicon.ico; serve the same SVG asset."""
    return send_file(
        os.path.join(app.root_path, "static", "favicon.svg"),
        mimetype="image/svg+xml",
        max_age=86400,
    )


@app.route("/register_service")
def register_service():
    return jsonify(
        {
            "name": "BR_exploreHD_DVR",
            "description": "Multi-camera MPEG-TS recorder for exploreHD / MCM RTSP streams",
            # BlueOS sidebar: MDI icon name only (see https://blueos.cloud/docs/latest/development/extensions/ ).
            "icon": "mdi-vhs",
            "company": "Blue Robotics",
            "version": VERSION,
            "webpage": "https://github.com/bluerobotics",
            "api": "",
        }
    )


@app.route("/status", methods=["GET"])
def route_status():
    with _state_lock:
        mgr = manager
        snap = list(streams_snapshot)
        err = boot_error
        stage = boot_stage
        sr = session_root
        sid = current_session_id
        usb = usb_storage.get_status()
    cams = mgr.status() if mgr else []
    recording = bool(
        mgr
        and not stopped_by_user
        and not _disk_stopped
        and any(c.get("state") == "running" for c in cams)
    )
    usb_free = usb.get("free_mb") if usb.get("mounted") else None
    telem = get_all_telemetry(recording_ok=recording, usb_disk_free_mb=usb_free)
    warn_streams = len(snap) > 0 and len(snap) < 4
    try:
        s = load_settings()
        auto_boot = bool(s.get("auto_record_on_boot", True))
        auto_dl_enabled = bool(s.get("auto_download_enabled", False))
        auto_dl_interval = int(s.get("auto_download_interval_minutes", 5))
    except Exception:
        auto_boot = True
        auto_dl_enabled = False
        auto_dl_interval = 5
    resp = jsonify(
        {
            "version": VERSION,
            "boot_stage": stage,
            "boot_error": err,
            "recording": recording,
            "stopped_by_user": stopped_by_user,
            "disk_stopped": _disk_stopped,
            "auto_record_on_boot": auto_boot,
            "auto_download_enabled": auto_dl_enabled,
            "auto_download_interval_minutes": auto_dl_interval,
            "streams_count": len(snap),
            "streams_warning": warn_streams,
            "session_root": sr,
            "session_id": sid,
            "cams": cams,
            "storage": {
                "recording_base": recording_base,
                "usb": usb,
            },
            "telemetry": telem,
        }
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/streams", methods=["GET"])
def route_streams():
    """Live MCM list for the Live tab (matches WebRTC); fallback to boot snapshot if MCM is down."""
    streams: List[Dict[str, Any]] = []
    try:
        streams = list_h264_rtsp_streams(base=MCM_BASE)
    except Exception as e:
        logger.warning("/streams: live MCM fetch failed: %s", e)
    if not streams:
        with _state_lock:
            streams = list(streams_snapshot)
    out = []
    for i, s in enumerate(streams):
        out.append(
            {
                "index": i,
                "name": s["name"],
                "stream_id": s["stream_id"],
                "rtsp_url": s["rtsp_url"],
                "webrtc_page": s.get("webrtc_page"),
                "mcm_root": s.get("mcm_root"),
            }
        )
    return jsonify(out)


def _mcm_all_running() -> Tuple[Optional[bool], List[Dict[str, Any]]]:
    """Return (all_running, raw_streams). all_running=None if MCM unreachable."""
    try:
        raw = fetch_streams_raw(base=MCM_BASE, timeout=2.5)
    except Exception as e:
        logger.info("/live/ensure_streams: MCM /streams fetch failed: %s", e)
        return None, []
    if not raw:
        return False, []
    return all(bool(s.get("running")) for s in raw), raw


@app.route("/live/ensure_streams", methods=["POST"])
def route_live_ensure_streams():
    """Ensure MCM has running pipelines so WebRTC `availableStreams` is non-empty.

    Idempotent and recording-safe: if every MCM stream already reports `running: true`, this is
    a no-op and will NOT restart pipelines (which would briefly disrupt active RTSP readers).
    Otherwise it calls MCM `POST /restart_streams?use_persistent=true` and polls briefly.
    """
    all_running, _raw = _mcm_all_running()
    if all_running is None:
        return jsonify({"success": False, "kicked": False, "message": "MCM unreachable"}), 503
    kicked = False
    if not all_running:
        kicked = kick_streams(base=MCM_BASE)
        deadline = time.monotonic() + 6.0
        while time.monotonic() < deadline:
            time.sleep(0.4)
            latest_all, _latest_raw = _mcm_all_running()
            if latest_all:
                all_running = True
                break
    try:
        streams = list_h264_rtsp_streams(base=MCM_BASE)
    except Exception as e:
        logger.warning("/live/ensure_streams: list failed after kick: %s", e)
        streams = []
    out = [
        {
            "index": i,
            "name": s["name"],
            "stream_id": s["stream_id"],
            "rtsp_url": s["rtsp_url"],
            "webrtc_page": s.get("webrtc_page"),
            "mcm_root": s.get("mcm_root"),
            "running": s.get("running", False),
        }
        for i, s in enumerate(streams)
    ]
    return jsonify({"success": bool(all_running), "kicked": kicked, "streams": out})


@app.route("/settings", methods=["GET"])
def route_settings_get():
    try:
        return jsonify(load_settings())
    except Exception as e:
        logger.exception("settings get failed")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/settings", methods=["POST"])
def route_settings_post():
    data = request.get_json(silent=True) or {}
    try:
        updates: Dict[str, Any] = {}
        if "auto_record_on_boot" in data:
            updates["auto_record_on_boot"] = bool(data["auto_record_on_boot"])
        if "auto_download_enabled" in data:
            updates["auto_download_enabled"] = bool(data["auto_download_enabled"])
        if "auto_download_interval_minutes" in data:
            # settings_store clamps; we let it through as-is so a JS string
            # (e.g. from a number input that round-trips as text) still parses.
            updates["auto_download_interval_minutes"] = data["auto_download_interval_minutes"]
        if not updates:
            return jsonify({"success": False, "message": "No recognized fields"}), 400
        merged = save_settings(updates)
        return jsonify({"success": True, "settings": merged})
    except Exception as e:
        logger.exception("settings post failed")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/tz", methods=["POST"])
def route_tz_post():
    """Persist the operator's browser TZ so segment timestamps and the calendar-day
    session directory match what they see in the UI.

    Body: {"tz_offset_minutes": <signed int, minutes east of UTC>, "tz_name": "<IANA>"}.
    The browser computes this as `-(new Date()).getTimezoneOffset()` (JS reports
    minutes WEST of UTC; we store EAST so the sign matches conventional usage).
    """
    data = request.get_json(silent=True) or {}
    if "tz_offset_minutes" not in data:
        return jsonify({"success": False, "message": "tz_offset_minutes required"}), 400
    try:
        merged = save_settings(
            {
                "browser_tz_offset_minutes": data.get("tz_offset_minutes"),
                "browser_tz_name": data.get("tz_name"),
            }
        )
        return jsonify(
            {
                "success": True,
                "tz_offset_minutes": merged.get("browser_tz_offset_minutes"),
                "tz_name": merged.get("browser_tz_name"),
            }
        )
    except Exception as e:
        logger.exception("tz post failed")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/start", methods=["POST"])
def route_start():
    global stopped_by_user, streams_snapshot, _disk_stopped
    stopped_by_user = False
    _disk_stopped = False
    streams, err = _retry_mcm_list()
    if err:
        return jsonify({"success": False, "message": err}), 503
    with _state_lock:
        sr = session_root
        mgr = manager
        rb = recording_base
    # 5 GB free gate for internal SD only. External media bypasses this check.
    if rb == RECORDINGS_LOCAL:
        free_mb = _disk_free_for_session()
        if free_mb is not None and free_mb < INTERNAL_SD_MIN_FREE_MB:
            return jsonify({
                "success": False,
                "message": (
                    f"Internal SD has only {free_mb:.0f} MB free; "
                    f"{INTERNAL_SD_MIN_FREE_MB // 1024} GB required to start recording. "
                    f"Free space or attach external media."
                ),
            }), 507  # Insufficient Storage
    if not sr:
        threading.Thread(target=_boot_worker, daemon=True, name="boot").start()
        return jsonify({"success": True, "message": "Boot sequence started"})
    if mgr:
        mgr.start_all()
        with _state_lock:
            streams_snapshot = streams
        return jsonify({"success": True, "message": "Recorders restarted"})
    return jsonify({"success": False, "message": "No session; wait for boot"}), 400


@app.route("/stop", methods=["POST"])
def route_stop():
    global manager, stopped_by_user
    stopped_by_user = True
    with _state_lock:
        mgr = manager
    if mgr:
        mgr.stop_all()
    return jsonify({"success": True})


@app.route("/cam/<int:index>/restart", methods=["POST"])
def route_cam_restart(index: int):
    with _state_lock:
        mgr = manager
    if not mgr or not mgr.restart_cam(index):
        return jsonify({"success": False, "message": "Invalid camera index"}), 404
    return jsonify({"success": True})


@app.route("/boot/retry", methods=["POST"])
def route_boot_retry():
    threading.Thread(target=_boot_worker, daemon=True, name="boot-retry").start()
    return jsonify({"success": True, "message": "Boot retry scheduled"})


def _walk_day(date_str: str) -> List[Tuple[str, str]]:
    """Return list of (full_path, archive_relative_path) for .ts and session .zip under date."""
    items: List[Tuple[str, str]] = []
    for root in _storage_roots():
        label = "sd" if root == RECORDINGS_LOCAL else "usb"
        day = os.path.join(root, date_str)
        if not os.path.isdir(day):
            continue
        for dirpath, _, files in os.walk(day):
            for fn in files:
                if fn.endswith(".ts") or fn.endswith(".zip"):
                    full = os.path.join(dirpath, fn)
                    rel = os.path.relpath(full, root)
                    arc = f"{label}/{rel}".replace("\\", "/")
                    items.append((full, arc))
    return items


def _walk_session(date_str: str, session_id: str) -> List[Tuple[str, str]]:
    """Return (full_path, archive_relative_path) tuples for one session across SD+USB.

    Used by the per-session download route. A session_id is a UUID-like name with
    no separators, so we reject anything that could traverse out of the date dir.
    """
    items: List[Tuple[str, str]] = []
    if "/" in session_id or "\\" in session_id or session_id in (".", ".."):
        return items
    for root in _storage_roots():
        label = "sd" if root == RECORDINGS_LOCAL else "usb"
        sp = os.path.join(root, date_str, session_id)
        if not os.path.isdir(sp):
            continue
        for dirpath, _, files in os.walk(sp):
            for fn in files:
                if fn.endswith(".ts") or fn.endswith(".zip"):
                    full = os.path.join(dirpath, fn)
                    rel = os.path.relpath(full, root)
                    arc = f"{label}/{rel}".replace("\\", "/")
                    items.append((full, arc))
    return items


# Filename written by the recorder watchdog when a segment closes:
#   YYYYMMDD_HHMMSS.ts                (normal case)
#   YYYYMMDD_HHMMSS_N.ts              (rare collision: two segments same wall second)
# The active segment is `seg_NNNNN.ts` and is intentionally excluded — auto-download
# only ships *closed* segments so we never zip a file the muxer is still writing.
_CLOSED_SEG_RE = re.compile(r"^(\d{8})_(\d{6})(?:_\d+)?\.ts$")
# Cam directories created by RecorderManager are `cam_<index>_<sanitized_name>`.
_CAM_DIR_RE = re.compile(r"^cam_(\d+)_(.+)$")


def _closed_segment_epoch(filename: str) -> Optional[float]:
    """Parse `YYYYMMDD_HHMMSS(_N)?.ts` -> UTC epoch seconds.

    Segment filenames are written in the operator's *browser-local* TZ
    (see recorder._format_segment_filename). We invert that here so callers
    can compare against a `since=<epoch>` query parameter consistently with
    `time.time()`. Falls back to local-TZ-naive interpretation when no
    browser has reported a TZ yet (matches `browser_local_datetime`).
    """
    m = _CLOSED_SEG_RE.match(filename)
    if not m:
        return None
    try:
        naive = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
    except ValueError:
        return None
    offset = get_browser_tz_offset_minutes()
    if offset is None:
        # Best-effort: treat the wall-clock string as the container's local time.
        # This is the same fallback browser_local_datetime uses; consistency
        # matters more than absolute correctness here because the cursor only
        # has to be monotonic relative to other segments named the same way.
        return naive.timestamp()
    tz = timezone(timedelta(minutes=offset))
    return naive.replace(tzinfo=tz).timestamp()


def _active_session_root_locked() -> Optional[str]:
    """Snapshot the active session_root under the state lock."""
    with _state_lock:
        return session_root


def walk_active_session_closed_segments(after_epoch: float = 0.0) -> List[Tuple[int, str, str, float]]:
    """Closed segments in the active session, optionally newer than `after_epoch`.

    Returns a list of `(cam_index, cam_name, ts_path, start_epoch)`, sorted by
    (start_epoch, cam_index) so the auto-download zip is naturally chronological
    across cameras. Returns an empty list when there is no active session
    (boot still pending, or boot failed without producing a session_root) or
    when nothing has been closed since `after_epoch`.

    `after_epoch` is *strict* greater-than: callers persist the latest epoch
    they have downloaded already and pass that value back so each segment is
    delivered exactly once.
    """
    sr = _active_session_root_locked()
    if not sr or not os.path.isdir(sr):
        return []
    out: List[Tuple[int, str, str, float]] = []
    try:
        cam_dirs = sorted(os.listdir(sr))
    except OSError:
        return []
    for cam_dir in cam_dirs:
        m = _CAM_DIR_RE.match(cam_dir)
        if not m:
            continue
        try:
            cam_index = int(m.group(1))
        except ValueError:
            continue
        cam_name = m.group(2)
        cam_path = os.path.join(sr, cam_dir)
        if not os.path.isdir(cam_path):
            continue
        try:
            entries = os.listdir(cam_path)
        except OSError:
            continue
        for fn in entries:
            epoch = _closed_segment_epoch(fn)
            if epoch is None:
                # Either the live `seg_NNNNN.ts` (still being written) or
                # an unrelated file — skip silently.
                continue
            if epoch <= after_epoch:
                continue
            out.append((cam_index, cam_name, os.path.join(cam_path, fn), epoch))
    out.sort(key=lambda t: (t[3], t[0]))
    return out


@app.route("/recordings", methods=["GET"])
def route_recordings():
    days_map: Dict[str, Dict[str, Any]] = {}
    for root in _storage_roots():
        label = "sd" if root == RECORDINGS_LOCAL else "usb"
        if not os.path.isdir(root):
            continue
        for date_name in os.listdir(root):
            if len(date_name) != 8 or not date_name.isdigit():
                continue
            date_path = os.path.join(root, date_name)
            if not os.path.isdir(date_path):
                continue
            if date_name not in days_map:
                days_map[date_name] = {
                    "date": date_name,
                    "total_bytes": 0,
                    "file_count": 0,
                    "sessions": [],
                    "storages": [],
                }
            for sess in sorted(os.listdir(date_path)):
                sp = os.path.join(date_path, sess)
                if not os.path.isdir(sp):
                    continue
                sess_bytes = 0
                sess_files = 0
                cams: List[Dict[str, Any]] = []
                for cam in sorted(os.listdir(sp)):
                    cp = os.path.join(sp, cam)
                    if not os.path.isdir(cp):
                        continue
                    segs = []
                    for fn in sorted(os.listdir(cp)):
                        if not fn.endswith(".ts"):
                            continue
                        fp = os.path.join(cp, fn)
                        sz = os.path.getsize(fp)
                        sess_bytes += sz
                        sess_files += 1
                        segs.append(
                            {
                                "name": fn,
                                "size_bytes": sz,
                                "download_url": f"/download/{date_name}/{sess}/{cam}/{fn}",
                            }
                        )
                    if segs:
                        cams.append({"name": cam, "segments": segs})
                for fn in os.listdir(sp):
                    if fn.endswith(".zip"):
                        fp = os.path.join(sp, fn)
                        sz = os.path.getsize(fp)
                        sess_bytes += sz
                        sess_files += 1
                if sess_bytes or cams:
                    days_map[date_name]["sessions"].append(
                        {
                            "id": sess,
                            "storage": label,
                            "bytes": sess_bytes,
                            "file_count": sess_files,
                            "cams": cams,
                        }
                    )
                    days_map[date_name]["total_bytes"] += sess_bytes
                    days_map[date_name]["file_count"] += sess_files
            if label not in days_map[date_name]["storages"]:
                days_map[date_name]["storages"].append(label)

    active_d = _active_session_date()
    out_days = []
    for d in sorted(days_map.keys(), reverse=True):
        entry = days_map[d]
        entry["active_session_here"] = bool(active_d == d and session_root)
        out_days.append(entry)
    return jsonify({"days": out_days})


def _safe_path_under(base: str, parts: List[str]) -> Optional[str]:
    candidate = os.path.realpath(os.path.join(base, *parts))
    base_real = os.path.realpath(base)
    if not candidate.startswith(base_real + os.sep) and candidate != base_real:
        return None
    return candidate


@app.route("/download/<date>/<session>/<cam>/<path:filename>", methods=["GET"])
def route_download_file(date: str, session: str, cam: str, filename: str):
    if not re.match(r"^\d{8}$", date) or ".." in session or ".." in cam:
        return jsonify({"success": False}), 400
    for root in _storage_roots():
        rel = _safe_path_under(root, [date, session, cam, filename])
        if rel and os.path.isfile(rel) and filename.endswith(".ts"):
            return send_file(rel, as_attachment=True, download_name=filename)
    return jsonify({"success": False, "message": "Not found"}), 404


class _ZipDrainBuffer(io.RawIOBase):
    """Write-only in-memory buffer used as zipfile.ZipFile's output stream.

    ZipFile detects this is non-seekable (tell() raises UnsupportedOperation)
    and switches to streaming mode: local headers use zip64 size placeholders
    and each entry is followed by a data descriptor with the real sizes/CRC.
    """

    def __init__(self):
        self._buf = bytearray()

    def writable(self) -> bool:
        return True

    def write(self, data) -> int:
        self._buf.extend(data)
        return len(data)

    def drain(self) -> bytes:
        if not self._buf:
            return b""
        out = bytes(self._buf)
        self._buf.clear()
        return out


def _estimated_zip_size(items: List[Tuple[str, str]]) -> int:
    # Upper-bound estimate for ZIP_STORED streaming with force_zip64. Real size is
    # within a few hundred bytes of this per entry and is only used for UI display.
    per_entry = 30 + 20 + 24 + 46 + 28  # local + zip64 extra + data descr + central + zip64 extra
    end = 22 + 20 + 56  # EOCD + zip64 EOCD locator + zip64 EOCD
    total = end
    for p, arc in items:
        try:
            sz = os.path.getsize(p)
        except OSError:
            sz = 0
        arc_len = len(arc.encode("utf-8"))
        total += per_entry + 2 * arc_len + sz
    return total


def _stream_zip(items: List[Tuple[str, str]], read_chunk: int = 1 << 20):
    """Yield a ZIP_STORED archive of the given (fullpath, arcname) items.

    - No compression: recordings are H264/MPEG-TS, which is already packed;
      ZIP_STORED is CPU-free and keeps I/O the only bottleneck.
    - Streams directly to the HTTP response — never writes a staging copy to
      /tmp. This halves disk I/O and makes the browser's download tray start
      showing bytes almost immediately instead of after multi-minute prep.
    - force_zip64=True so a single >4 GiB segment (or a total archive >4 GiB)
      is handled correctly without pre-computing sizes.
    """
    buf = _ZipDrainBuffer()
    zf = zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True)
    try:
        for full, arc in items:
            try:
                zi = zipfile.ZipInfo.from_file(full, arcname=arc)
            except FileNotFoundError:
                # A segment can finalize-and-rename between /recordings listing and
                # the actual stream; skip gracefully so the archive remains usable.
                logger.warning("download: skipping missing %s", full)
                continue
            zi.compress_type = zipfile.ZIP_STORED
            try:
                with open(full, "rb") as src, zf.open(zi, mode="w", force_zip64=True) as dest:
                    while True:
                        chunk = src.read(read_chunk)
                        if not chunk:
                            break
                        dest.write(chunk)
                        data = buf.drain()
                        if data:
                            yield data
            except FileNotFoundError:
                logger.warning("download: segment vanished mid-stream: %s", full)
                continue
            except OSError as e:
                logger.warning("download: read error for %s: %s", full, e)
                continue
            # Data descriptor / bookkeeping bytes emitted by ZipFile on close.
            data = buf.drain()
            if data:
                yield data
    finally:
        zf.close()
    tail = buf.drain()
    if tail:
        yield tail


def _collect_items_for_dates(dates: List[str]) -> Tuple[List[Tuple[str, str]], List[str]]:
    bad: List[str] = []
    items: List[Tuple[str, str]] = []
    for d in dates:
        ds = str(d)
        if not re.match(r"^\d{8}$", ds):
            bad.append(ds)
            continue
        items.extend(_walk_day(ds))
    return items, bad


def _download_response(items: List[Tuple[str, str]], filename: str) -> Response:
    total_bytes = 0
    for p, _ in items:
        try:
            total_bytes += os.path.getsize(p)
        except OSError:
            pass
    resp = Response(_stream_zip(items), mimetype="application/zip")
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    # Custom headers so the UI can show an accurate preparation summary even when
    # clients use a direct navigation (browser download manager) and don't see
    # a JSON body.
    resp.headers["X-Filename"] = filename
    resp.headers["X-File-Count"] = str(len(items))
    resp.headers["X-Raw-Bytes"] = str(total_bytes)
    resp.headers["X-Estimated-Bytes"] = str(_estimated_zip_size(items))
    # Chunked transfer: no Content-Length. Browsers render bytes-downloaded and
    # throughput in the native download tray, which is enough feedback here.
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/recordings/download_info", methods=["POST"])
def route_download_info():
    """Preflight for the UI: how many files and bytes will a given selection zip up?

    Kept cheap (os.path.getsize only, no reads) so the modal shows info within a
    few hundred ms even for large selections.
    """
    data = request.get_json(silent=True) or {}
    dates = data.get("dates") or []
    if not isinstance(dates, list) or not dates:
        return jsonify({"success": False, "message": "dates[] required"}), 400
    items, bad = _collect_items_for_dates(dates)
    if bad:
        return jsonify({"success": False, "message": f"Bad date(s): {','.join(bad)}"}), 400
    total_bytes = 0
    for p, _ in items:
        try:
            total_bytes += os.path.getsize(p)
        except OSError:
            pass
    return jsonify(
        {
            "success": True,
            "dates": [str(d) for d in dates],
            "file_count": len(items),
            "total_bytes": total_bytes,
            "estimated_zip_bytes": _estimated_zip_size(items),
        }
    )


@app.route("/download_day/<date>", methods=["GET"])
def route_download_day(date: str):
    if not re.match(r"^\d{8}$", date):
        return jsonify({"success": False}), 400
    items = _walk_day(date)
    if not items:
        return jsonify({"success": False, "message": "No files"}), 404
    return _download_response(items, f"BR_exploreHD_DVR_{date}.zip")


@app.route("/download_session/<date>/<session>", methods=["GET"])
def route_download_session(date: str, session: str):
    """Download a single session as one .zip.

    Completed sessions already have a pre-built `<sessionId>.zip` written by
    boot_manager; serving that file directly avoids re-zipping multi-GB
    archives just to deliver bytes the OS already has on disk. For sessions
    that don't have a prebuilt zip yet (the active one, or a session whose
    boot-zip hasn't run on this storage), we fall back to streaming a
    ZIP_STORED archive of the .ts segments — same path the day-zip uses.
    """
    if not re.match(r"^\d{8}$", date):
        return jsonify({"success": False, "message": "Bad date"}), 400
    if "/" in session or "\\" in session or session in (".", ".."):
        return jsonify({"success": False, "message": "Bad session"}), 400
    download_name = f"BR_exploreHD_DVR_{date}_{session}.zip"
    for root in _storage_roots():
        zp = os.path.join(root, date, session, f"{session}.zip")
        if os.path.isfile(zp):
            return send_file(zp, as_attachment=True, download_name=download_name)
    items = _walk_session(date, session)
    if not items:
        return jsonify({"success": False, "message": "No files"}), 404
    return _download_response(items, download_name)


def _dates_from_query() -> List[str]:
    """Accept either ?d=YYYYMMDD&d=... or ?dates=YYYYMMDD,YYYYMMDD."""
    dates: List[str] = list(request.args.getlist("d"))
    if not dates:
        raw = request.args.get("dates", "")
        if raw:
            dates = [x for x in raw.split(",") if x]
    return dates


@app.route("/download_days", methods=["GET", "POST"])
def route_download_days():
    # GET with query params lets the UI use a plain <a> navigation so the
    # browser's native download manager handles progress/pause/resume and we
    # never have to buffer the archive in JS memory.
    if request.method == "GET":
        dates = _dates_from_query()
    else:
        data = request.get_json(silent=True) or {}
        dates = data.get("dates") or []
    if not isinstance(dates, list) or not dates:
        return jsonify({"success": False, "message": "dates[] required"}), 400
    items, bad = _collect_items_for_dates(dates)
    if bad:
        return jsonify({"success": False, "message": f"Bad date(s): {','.join(bad)}"}), 400
    if not items:
        return jsonify({"success": False, "message": "No files for selection"}), 404
    filename = f"BR_exploreHD_DVR_multi_{int(time.time())}.zip"
    return _download_response(items, filename)


def _perform_delete_dates(dates: List[str]):
    if not isinstance(dates, list) or not dates:
        return jsonify({"success": False, "message": "dates[] required"}), 400
    active_d = _active_session_date()
    active_session_name = os.path.basename(session_root) if session_root else None
    active_session_abspath = os.path.abspath(session_root) if session_root else None
    deleted: List[str] = []
    partial: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []
    for d in dates:
        ds = str(d)
        if not re.match(r"^\d{8}$", ds):
            skipped.append({"date": ds, "reason": "invalid date"})
            continue
        is_active_day = (active_d == ds and active_session_abspath and os.path.isdir(active_session_abspath))
        removed_any = False
        kept_active = False
        for root in _storage_roots():
            day_path = os.path.join(root, ds)
            if not os.path.isdir(day_path):
                continue
            # On the active session's calendar day, only preserve the one live session
            # directory; delete every other session (ended zips, ended dirs, stale folders)
            # on every storage root. Previously we skipped the whole day, which made the
            # UI appear broken when users tried to prune today's old sessions.
            if is_active_day and os.path.abspath(day_path) == os.path.dirname(active_session_abspath):
                for entry in os.listdir(day_path):
                    # Keep the live session directory itself. Its matching <id>.zip
                    # shouldn't exist yet (zipping happens post-session), but guard anyway.
                    if active_session_name and (
                        entry == active_session_name
                        or entry == f"{active_session_name}.zip"
                    ):
                        kept_active = True
                        continue
                    p = os.path.join(day_path, entry)
                    try:
                        if os.path.isdir(p):
                            shutil.rmtree(p)
                        else:
                            os.remove(p)
                        removed_any = True
                    except OSError as e:
                        logger.warning("delete: failed to remove %s: %s", p, e)
            else:
                try:
                    shutil.rmtree(day_path)
                    removed_any = True
                except OSError as e:
                    logger.warning("delete: failed to remove %s: %s", day_path, e)
        if removed_any and kept_active:
            partial.append({"date": ds, "reason": "kept active session; other recordings removed"})
        elif removed_any:
            deleted.append(ds)
        elif kept_active:
            # Only the active session exists on this day and nothing else to remove.
            skipped.append({"date": ds, "reason": "only active session present"})
        else:
            skipped.append({"date": ds, "reason": "not found"})
    return jsonify({"success": True, "deleted": deleted, "partial": partial, "skipped": skipped})


@app.route("/recordings/delete", methods=["POST"])
def route_recordings_delete():
    data = request.get_json(silent=True) or {}
    dates = data.get("dates") or []
    return _perform_delete_dates(dates)


@app.route("/recordings/<date>", methods=["DELETE"])
def route_delete_day(date: str):
    return _perform_delete_dates([date])


# ---------------------------------------------------------------------------
# Auto-download: periodic per-tab download of recently-finalized segments,
# plus a status .txt operators get on warm-up and on ticks with no new
# segments. See app/static/index.html (Status tab) for the front-end.
# ---------------------------------------------------------------------------


def _device_kind(device_path: Optional[str]) -> str:
    """Friendly storage label inferred from a /dev/* node.

    BlueOS extension hosts may have either an NVMe HAT (`/dev/nvme0n1pN`) or a
    USB stick / USB-NVMe enclosure (`/dev/sdaN`). The recordings tree is the
    same in both cases (USB_MOUNT_POINT/DVR_DIR), but operators reading the
    status text want to know which medium they're actually filling up.
    """
    if not device_path:
        return "unknown"
    base = os.path.basename(device_path)
    if base.startswith("nvme"):
        return "NVMe"
    if base.startswith("sd"):
        return "USB/SATA"
    return "external"


def _format_tz_label(now_local: datetime) -> str:
    """Render `<TZ name> (UTC±H[:MM])` for the status .txt header.

    Uses the operator's last-reported browser TZ name when available; falls
    back to whatever `tzinfo` is attached to `now_local`. Operators tend to
    spot-check the TZ string before trusting timestamps in incident reviews,
    so we keep it explicit on every report.
    """
    try:
        s = load_settings()
    except Exception:
        s = {}
    tz_name = s.get("browser_tz_name") or ""
    tz = now_local.tzinfo
    offset_str = "UTC"
    if tz is not None:
        off = tz.utcoffset(now_local)
        if off is not None:
            total_min = int(off.total_seconds() // 60)
            sign = "+" if total_min >= 0 else "-"
            total_min = abs(total_min)
            hh, mm = divmod(total_min, 60)
            offset_str = f"UTC{sign}{hh}" + (f":{mm:02d}" if mm else "")
    if tz_name:
        return f"{tz_name} ({offset_str})"
    return offset_str


def _build_status_text() -> str:
    """Render the human-readable telemetry snapshot delivered alongside auto-downloads.

    Pulls from the same sources `/status` already aggregates (system_telemetry,
    usb_storage, the recorder manager, persistent settings). Lines are
    `KEY: VALUE` for easy grep/diff across reports — operators stash these
    files alongside the recordings as a per-segment health log.
    """
    now_local = browser_local_datetime()
    tz_label = _format_tz_label(now_local)
    header_time = now_local.strftime("%Y-%m-%d %H:%M:%S")

    with _state_lock:
        mgr = manager
        snap = list(streams_snapshot)
        err = boot_error
        stage = boot_stage
        sr = session_root
        sid = current_session_id
        rb = recording_base
        stopped = stopped_by_user
        disk_stop = _disk_stopped
        usb = usb_storage.get_status()
    cams = mgr.status() if mgr else []
    recording = bool(
        mgr
        and not stopped
        and not disk_stop
        and any(c.get("state") == "running" for c in cams)
    )
    usb_free = usb.get("free_mb") if usb.get("mounted") else None
    telem = get_all_telemetry(recording_ok=recording, usb_disk_free_mb=usb_free)

    streams_count = len(snap)
    if streams_count == 0:
        streams_line = "0 of 4 — no H264 RTSP streams discovered"
    elif streams_count < 4:
        streams_line = f"{streams_count} of 4 — fewer streams than expected (warning)"
    else:
        streams_line = f"{streams_count} of 4 (no warning)"

    out: List[str] = []
    out.append(f"BR_exploreHD_DVR status — generated {header_time} {tz_label}")
    out.append(f"Version: {VERSION}")
    out.append(
        f"Boot stage: {stage}"
        + (f"   error: {err}" if err else "")
    )
    out.append(
        f"Recording: {str(recording).lower()}   "
        f"stopped_by_user: {str(stopped).lower()}   "
        f"disk_stopped: {str(disk_stop).lower()}"
    )
    out.append(f"Session: {sid or '—'}   path: {sr or '—'}")
    out.append(f"Streams: {streams_line}")
    out.append("")
    out.append("--- System ---")

    def _fmt(v: Any, unit: str = "") -> str:
        if v is None:
            return "—"
        return f"{v}{unit}" if unit else str(v)

    out.append(f"CPU temperature: {_fmt(telem.get('cpu_temp_c'), ' °C')}")
    out.append(f"CPU voltage: {_fmt(telem.get('cpu_voltage_v'), ' V' if isinstance(telem.get('cpu_voltage_v'), (int, float)) else '')}")
    out.append(f"CPU clock: {_fmt(telem.get('cpu_clock_mhz'), ' MHz')}")
    out.append(f"CPU load (1m): {_fmt(telem.get('cpu_load_avg'))}")
    out.append(f"Time synced: {_fmt(telem.get('time_synced'))}")
    out.append(f"System time: {_fmt(telem.get('system_time'))}")
    out.append("")
    out.append("--- Storage ---")
    out.append(
        f"Internal SD: {_fmt(telem.get('disk_free_mb'), ' MB free')}"
        f" (recording base: {rb})"
    )
    if usb.get("mounted"):
        out.append(
            f"External media: mounted on {usb.get('mount_point')}, "
            f"device {usb.get('device')}, "
            f"{_fmt(usb.get('free_mb'), ' MB free')}, "
            f"usable: {str(bool(usb.get('usable'))).lower()}"
        )
        out.append(f"Device kind: {_device_kind(usb.get('device'))}")
    else:
        out.append("External media: not mounted")
    out.append("")
    out.append("--- Cameras ---")
    if not cams:
        out.append("(no recorders running)")
    else:
        for c in cams:
            line = (
                f"cam{c.get('index')} \"{c.get('name')}\"   "
                f"state={c.get('state')}  "
                f"current_segment={c.get('current_segment') or '—'}"
                f" ({c.get('current_segment_mb')} MB)  "
                f"session_total={c.get('session_total_mb')} MB  "
                f"restarts={c.get('restart_count')}  "
                f"gst_errors={c.get('gst_errors')}  "
                f"last_error={(c.get('last_error') or '').strip()!r}"
            )
            out.append(line)
    out.append("")
    return "\n".join(out)


def _status_txt_filename(now_local: Optional[datetime] = None) -> str:
    if now_local is None:
        now_local = browser_local_datetime()
    return "BR_exploreHD_DVR_status_" + now_local.strftime("%Y%m%d_%H%M%S") + ".txt"


def _status_txt_response() -> Response:
    """Plain `text/plain` download with the latest status .txt as the body.

    Used for both warm-up requests (Chrome permission gesture) and for periodic
    auto-download ticks where no new segments have closed since the last cursor.
    """
    now_local = browser_local_datetime()
    body = _build_status_text()
    fname = _status_txt_filename(now_local)
    # Flask auto-appends `; charset=utf-8` to any text/* mimetype, so we pass
    # a bare `text/plain` here and avoid the doubled-charset bug we saw when
    # both Flask and our explicit charset landed in the same header.
    resp = Response(body, mimetype="text/plain")
    resp.headers["Content-Disposition"] = f'attachment; filename="{fname}"'
    resp.headers["Cache-Control"] = "no-store"
    return resp


# ffmpeg remux command. `-c copy` keeps the H264 elementary stream untouched
# (no re-encode, ~0% CPU on Pi) and `frag_keyframe+empty_moov` writes a
# fragmented MP4 so the muxer doesn't need to seek backwards to write the
# moov atom — that's what lets us pipe stdout directly into the zip stream.
def _ffmpeg_remux_cmd(ts_path: str) -> List[str]:
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-i", ts_path,
        "-c", "copy",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        "-f", "mp4",
        "pipe:1",
    ]


def _sanitize_zip_entry(name: str) -> str:
    """Strip path separators / control chars from a string before using it in a zip entry."""
    return re.sub(r"[\x00-\x1f/\\]+", "_", name).strip("._") or "cam"


def _stream_auto_download_zip(
    items: List[Tuple[int, str, str, float]],
    status_text: str,
    status_filename: str,
    read_chunk: int = 1 << 20,
):
    """Stream a ZIP_STORED archive of MP4s + a status .txt entry.

    Reuses the same _ZipDrainBuffer non-seekable streaming trick as
    `_stream_zip`, but per-segment we pipe ffmpeg's stdout into the open
    zip entry instead of copying bytes from disk. force_zip64=True so a
    single oversized segment doesn't blow past the 4 GiB ZIP_STORED cap.
    """
    buf = _ZipDrainBuffer()
    zf = zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True)
    try:
        # Status .txt first so a `unzip -p archive.zip status_*.txt` gives
        # operators an instant summary without parsing the whole archive.
        try:
            txt_bytes = status_text.encode("utf-8")
            zi = zipfile.ZipInfo(status_filename)
            zi.compress_type = zipfile.ZIP_STORED
            zi.date_time = datetime.now().timetuple()[:6]
            with zf.open(zi, mode="w", force_zip64=True) as dest:
                dest.write(txt_bytes)
            data = buf.drain()
            if data:
                yield data
        except Exception as e:
            logger.warning("auto-download: failed to embed status .txt: %s", e)

        for cam_index, cam_name, ts_path, start_epoch in items:
            entry_name = (
                f"cam{cam_index}_{_sanitize_zip_entry(cam_name)}_"
                + browser_local_datetime(start_epoch).strftime("%Y%m%d_%H%M%S")
                + ".mp4"
            )
            zi = zipfile.ZipInfo(entry_name)
            zi.compress_type = zipfile.ZIP_STORED
            zi.date_time = datetime.now().timetuple()[:6]

            proc: Optional[subprocess.Popen] = None
            try:
                proc = subprocess.Popen(
                    _ffmpeg_remux_cmd(ts_path),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                )
            except Exception as e:
                logger.warning("auto-download: failed to spawn ffmpeg for %s: %s", ts_path, e)
                continue

            try:
                with zf.open(zi, mode="w", force_zip64=True) as dest:
                    while True:
                        chunk = proc.stdout.read(read_chunk) if proc.stdout else b""
                        if not chunk:
                            break
                        dest.write(chunk)
                        data = buf.drain()
                        if data:
                            yield data
                # Drain the entry's data descriptor / bookkeeping bytes.
                data = buf.drain()
                if data:
                    yield data
                rc = proc.wait(timeout=60)
                if rc != 0:
                    err_blob = b""
                    try:
                        if proc.stderr:
                            err_blob = proc.stderr.read() or b""
                    except Exception:
                        pass
                    logger.warning(
                        "auto-download: ffmpeg rc=%s for %s: %s",
                        rc, ts_path, err_blob.decode(errors="replace").strip()
                    )
            except Exception as e:
                logger.warning("auto-download: remux failed for %s: %s", ts_path, e)
                # Best-effort: kill stuck ffmpeg so we don't leak processes.
                try:
                    if proc and proc.poll() is None:
                        proc.kill()
                except Exception:
                    pass
                continue
    finally:
        zf.close()
    tail = buf.drain()
    if tail:
        yield tail


@app.route("/auto_download_zip/info", methods=["GET"])
def route_auto_download_info():
    """Cheap preflight: how many new segments are waiting after `since=<epoch>`?

    The front-end calls this *before* the actual download so it can advance the
    persistent localStorage cursor to the highest segment epoch in the upcoming
    zip. The hidden-`<a>` download approach we use to avoid buffering multi-GB
    archives in JS memory cannot read response headers, so we expose the cursor
    over a tiny JSON endpoint instead.
    """
    try:
        since = float(request.args.get("since", "0"))
    except (TypeError, ValueError):
        since = 0.0
    items = walk_active_session_closed_segments(after_epoch=since)
    total_bytes = 0
    latest = since
    for _idx, _name, path, epoch in items:
        try:
            total_bytes += os.path.getsize(path)
        except OSError:
            pass
        if epoch > latest:
            latest = epoch
    return jsonify(
        {
            "has_new": bool(items),
            "latest_epoch": latest,
            "file_count": len(items),
            "total_bytes": total_bytes,
        }
    )


@app.route("/auto_download_zip", methods=["GET"])
def route_auto_download_zip():
    """Periodic auto-download endpoint.

    Behavior matrix:
      - `?warmup=1` → plain `text/plain` status report. Used by the front-end
        on the user's enable-click so Chrome counts it as a user-gesture
        download and surfaces the "Allow multiple downloads" permission prompt.
      - No new segments since `?since=<epoch>` → same plain text/plain status
        report so every periodic tick still produces a useful download (a
        per-tick health pulse) and Chrome doesn't see a 204 it could mistake
        for a failed download.
      - New segments → streaming `application/zip` containing the closed `.ts`
        segments remuxed to `.mp4` (ffmpeg `-c copy`, no re-encode), plus a
        `status_<timestamp>.txt` entry at the top of the archive.
    """
    warmup = request.args.get("warmup", "0").strip() in ("1", "true", "yes")
    if warmup:
        return _status_txt_response()

    try:
        since = float(request.args.get("since", "0"))
    except (TypeError, ValueError):
        since = 0.0

    items = walk_active_session_closed_segments(after_epoch=since)
    if not items:
        # Nothing new — give the operator a status .txt so the tick is still
        # productive (and so Chrome's per-origin auto-download permission stays
        # warm).
        return _status_txt_response()

    now_local = browser_local_datetime()
    status_filename = _status_txt_filename(now_local)
    status_text = _build_status_text()
    zip_filename = "BR_exploreHD_DVR_auto_" + now_local.strftime("%Y%m%d_%H%M%S") + ".zip"
    latest_epoch = max(epoch for _i, _n, _p, epoch in items)

    resp = Response(
        _stream_auto_download_zip(items, status_text, status_filename),
        mimetype="application/zip",
    )
    resp.headers["Content-Disposition"] = f'attachment; filename="{zip_filename}"'
    # Cursor-advance hint for clients that *can* read response headers (e.g.
    # the /info preflight is more reliable, but this is here for parity with
    # the existing /download_* routes).
    resp.headers["X-Latest-Closed-Epoch"] = f"{latest_epoch:.6f}"
    resp.headers["X-File-Count"] = str(len(items))
    resp.headers["Cache-Control"] = "no-store"
    return resp


if __name__ == "__main__":
    threading.Thread(target=_boot_worker, daemon=True, name="boot").start()
    # Default 6010: free next to MCM (6020/6021/6030/6040); 5777 is mavlink-server on BlueOS.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "6010")))