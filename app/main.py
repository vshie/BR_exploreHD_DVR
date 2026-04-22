"""
BR_exploreHD_DVR — BlueOS extension: record MCM H264 RTSP streams to segmented MPEG-TS.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import threading
import time
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, after_this_request, jsonify, request, send_file

import usb_storage
from boot_manager import SEGMENT_SECONDS_DEFAULT, run_boot_sequence
from mcm_client import DEFAULT_MCM_BASE, list_h264_rtsp_streams
from recorder import RecorderManager
from settings_store import load_settings, save_settings
from system_telemetry import get_all_telemetry, get_disk_free_mb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

RECORDINGS_LOCAL = "/app/recordings"
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
            mgr = RecorderManager(sr, streams, segment_ns, _disk_free_for_session, on_disk_critical=_on_disk_critical)
            auto_rec = bool(load_settings().get("auto_record_on_boot", True))
            with _state_lock:
                manager = mgr
                boot_stage = "recording" if auto_rec else "standby"
            if auto_rec:
                mgr.start_all()
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
            "version": "1.0.7",
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


@app.route("/download_day/<date>", methods=["GET"])
def route_download_day(date: str):
    if not re.match(r"^\d{8}$", date):
        return jsonify({"success": False}), 400
    items = _walk_day(date)
    if not items:
        return jsonify({"success": False, "message": "No files"}), 404
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir="/tmp")
    tmp.close()
    try:
        with zipfile.ZipFile(tmp.name, "w", compression=zipfile.ZIP_STORED) as zf:
            for full, arc in items:
                zf.write(full, arcname=arc, compress_type=zipfile.ZIP_STORED)

        def _cleanup_day(resp):
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            return resp

        after_this_request(_cleanup_day)
        return send_file(
            tmp.name,
            as_attachment=True,
            download_name=f"BR_exploreHD_DVR_{date}.zip",
            mimetype="application/zip",
        )
    except Exception as e:
        logger.exception("download_day zip failed")
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/download_days", methods=["POST"])
def route_download_days():
    data = request.get_json(silent=True) or {}
    dates = data.get("dates") or []
    if not isinstance(dates, list) or not dates:
        return jsonify({"success": False, "message": "dates[] required"}), 400
    for d in dates:
        if not re.match(r"^\d{8}$", str(d)):
            return jsonify({"success": False, "message": f"Bad date: {d}"}), 400
    all_items: List[Tuple[str, str]] = []
    for d in dates:
        all_items.extend(_walk_day(str(d)))
    if not all_items:
        return jsonify({"success": False, "message": "No files for selection"}), 404
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir="/tmp")
    tmp.close()
    try:
        with zipfile.ZipFile(tmp.name, "w", compression=zipfile.ZIP_STORED) as zf:
            for full, arc in all_items:
                zf.write(full, arcname=arc, compress_type=zipfile.ZIP_STORED)

        def _cleanup_days(resp):
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            return resp

        after_this_request(_cleanup_days)
        return send_file(
            tmp.name,
            as_attachment=True,
            download_name=f"BR_exploreHD_DVR_multi_{int(time.time())}.zip",
            mimetype="application/zip",
        )
    except Exception as e:
        logger.exception("download_days zip failed")
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        return jsonify({"success": False, "message": str(e)}), 500


def _perform_delete_dates(dates: List[str]):
    if not isinstance(dates, list) or not dates:
        return jsonify({"success": False, "message": "dates[] required"}), 400
    active_d = _active_session_date()
    deleted = []
    skipped = []
    for d in dates:
        ds = str(d)
        if not re.match(r"^\d{8}$", ds):
            skipped.append({"date": ds, "reason": "invalid date"})
            continue
        if active_d == ds and session_root and os.path.isdir(session_root):
            skipped.append({"date": ds, "reason": "active session on this calendar day"})
            continue
        removed_any = False
        for root in _storage_roots():
            day_path = os.path.join(root, ds)
            if os.path.isdir(day_path):
                shutil.rmtree(day_path)
                removed_any = True
        if removed_any:
            deleted.append(ds)
        else:
            skipped.append({"date": ds, "reason": "not found"})
    return jsonify({"success": True, "deleted": deleted, "skipped": skipped})


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