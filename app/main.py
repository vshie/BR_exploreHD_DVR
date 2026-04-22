"""
BR_exploreHD_DVR — BlueOS extension: record MCM H264 RTSP streams to segmented MPEG-TS.
"""

from __future__ import annotations

import io
import logging
import os
import re
import shutil
import threading
import time
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, request, send_file

import usb_storage
from boot_manager import SEGMENT_SECONDS_DEFAULT, run_boot_sequence
from mcm_client import DEFAULT_MCM_BASE, fetch_streams_raw, kick_streams, list_h264_rtsp_streams
from recorder import RecorderManager
from settings_store import load_settings, save_settings
from system_telemetry import get_all_telemetry, get_disk_free_mb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

VERSION = "1.0.20"

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
        auto_boot = bool(load_settings().get("auto_record_on_boot", True))
    except Exception:
        auto_boot = True
    resp = jsonify(
        {
            "version": VERSION,
            "boot_stage": stage,
            "boot_error": err,
            "recording": recording,
            "stopped_by_user": stopped_by_user,
            "disk_stopped": _disk_stopped,
            "auto_record_on_boot": auto_boot,
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
        if not updates:
            return jsonify({"success": False, "message": "No recognized fields"}), 400
        merged = save_settings(updates)
        return jsonify({"success": True, "settings": merged})
    except Exception as e:
        logger.exception("settings post failed")
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


if __name__ == "__main__":
    threading.Thread(target=_boot_worker, daemon=True, name="boot").start()
    # Default 6010: free next to MCM (6020/6021/6030/6040); 5777 is mavlink-server on BlueOS.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "6010")))