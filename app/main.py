"""
BR_exploreHD_DVR — BlueOS extension: relay MCM H264 RTSP streams to a hardcoded
RTMP endpoint. Cloud-only build (no disk recording).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, request, send_file

import cloud_relay
from boot_manager import run_boot_sequence
from mcm_client import DEFAULT_MCM_BASE, fetch_streams_raw, kick_streams, list_h264_rtsp_streams
from settings_store import load_settings, save_settings
from system_telemetry import get_all_telemetry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

VERSION = "1.0.41"

MCM_BASE = os.environ.get("MCM_BASE", DEFAULT_MCM_BASE).rstrip("/")

_boot_lock = threading.Lock()
_state_lock = threading.Lock()

boot_stage = "starting"
boot_error: Optional[str] = None
streams_snapshot: List[Dict[str, Any]] = []


def _set_boot_stage(stage: str) -> None:
    global boot_stage
    with _state_lock:
        boot_stage = stage


def _current_streams_snapshot() -> List[Dict[str, Any]]:
    """Provider callback for cloud_relay: hand back the latest MCM stream list
    so a toggle-on after boot doesn't need to re-run the boot sequence."""
    with _state_lock:
        return list(streams_snapshot)


def _start_cloud_from_boot_streams(streams: List[Dict[str, Any]]) -> None:
    """Cloud fast-path: start RTMP as soon as MCM lists streams."""
    global streams_snapshot
    with _state_lock:
        streams_snapshot = list(streams)
    try:
        cloud_relay.configure(_current_streams_snapshot)
        cloud_relay.start_if_enabled()
        logger.info("Cloud relay started on MCM fast-path (%d stream(s))", len(streams))
    except Exception:
        logger.exception("Cloud relay failed to start on MCM fast-path")


def _boot_worker():
    global boot_stage, boot_error, streams_snapshot
    with _boot_lock:
        try:
            streams, err, stage = run_boot_sequence(
                MCM_BASE,
                on_stage=_set_boot_stage,
                on_streams=_start_cloud_from_boot_streams,
            )
            with _state_lock:
                boot_stage = stage
                streams_snapshot = list(streams)
                boot_error = err
            if err:
                logger.error(err)
                return
            # Idempotent: on_streams already started the relay. This covers the
            # case where the relay was disabled at boot and gets toggled on
            # later — same provider is already configured.
            try:
                cloud_relay.configure(_current_streams_snapshot)
                cloud_relay.start_if_enabled()
            except Exception:
                logger.exception("Cloud relay start_if_enabled failed at boot")
        except Exception as e:
            logger.exception("Boot worker failed")
            with _state_lock:
                boot_error = str(e)
                boot_stage = "error"


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/favicon.ico")
def favicon_ico():
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
            "description": "Cloud RTMP relay + Live view for exploreHD / MCM RTSP streams",
            "icon": "mdi-cloud-upload",
            "company": "Blue Robotics",
            "version": VERSION,
            "webpage": "https://github.com/bluerobotics",
            "api": "",
        }
    )


@app.route("/status", methods=["GET"])
def route_status():
    with _state_lock:
        snap = list(streams_snapshot)
        err = boot_error
        stage = boot_stage
    telem = get_all_telemetry()
    warn_streams = len(snap) > 0 and len(snap) < 4
    try:
        cloud_summary = cloud_relay.summary()
    except Exception:
        logger.exception("cloud_relay summary failed")
        cloud_summary = {
            "enabled": True,
            "running": False,
            "streaming_count": 0,
            "total_count": 0,
            "total_restarts": 0,
            "rtmp_base_url": cloud_relay.RTMP_BASE_URL,
        }
    resp = jsonify(
        {
            "version": VERSION,
            "boot_stage": stage,
            "boot_error": err,
            "streams_count": len(snap),
            "streams_warning": warn_streams,
            "telemetry": telem,
            "cloud": cloud_summary,
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

    Idempotent: if every MCM stream already reports `running: true`, this is a
    no-op (which is also what we want so a Live-tab click doesn't disturb
    the RTSP feed the cloud relay is reading). Otherwise it calls MCM
    `POST /restart_streams?use_persistent=true` and polls briefly.
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
        if "cloud_relay_enabled" in data:
            updates["cloud_relay_enabled"] = bool(data["cloud_relay_enabled"])
        if not updates:
            return jsonify({"success": False, "message": "No recognized fields"}), 400
        merged = save_settings(updates)
        if "cloud_relay_enabled" in updates:
            try:
                cloud_relay.apply_settings_change()
            except Exception:
                logger.exception("cloud_relay apply_settings_change failed (settings POST)")
        return jsonify({"success": True, "settings": merged})
    except Exception as e:
        logger.exception("settings post failed")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/cloud/status", methods=["GET"])
def route_cloud_status():
    """Cloud relay payload for the Cloud tab: per-cam RTMP state, restarts, errors."""
    try:
        return jsonify({"cloud": cloud_relay.status()})
    except Exception as e:
        logger.exception("cloud status failed")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/cloud/toggle", methods=["POST"])
def route_cloud_toggle():
    """Flip the persisted cloud-relay toggle and start/stop the relay.

    Body: `{"enabled": bool}`. Accepts `cloud_relay_enabled` for parity with
    the unified `/settings` POST.
    """
    data = request.get_json(silent=True) or {}
    if "enabled" in data:
        new = bool(data["enabled"])
    elif "cloud_relay_enabled" in data:
        new = bool(data["cloud_relay_enabled"])
    else:
        return jsonify({"success": False, "message": "enabled required"}), 400
    try:
        merged = save_settings({"cloud_relay_enabled": new})
    except Exception as e:
        logger.exception("cloud toggle save failed")
        return jsonify({"success": False, "message": str(e)}), 500
    try:
        cloud_relay.apply_settings_change()
    except Exception as e:
        logger.exception("cloud_relay apply_settings_change failed")
        return jsonify({
            "success": False,
            "message": f"Setting saved but relay refresh failed: {e}",
            "cloud_relay_enabled": merged.get("cloud_relay_enabled", new),
        }), 500
    return jsonify({
        "success": True,
        "cloud_relay_enabled": merged.get("cloud_relay_enabled", new),
        "cloud": cloud_relay.summary(),
    })


@app.route("/boot/retry", methods=["POST"])
def route_boot_retry():
    threading.Thread(target=_boot_worker, daemon=True, name="boot-retry").start()
    return jsonify({"success": True, "message": "Boot retry scheduled"})


if __name__ == "__main__":
    threading.Thread(target=_boot_worker, daemon=True, name="boot").start()
    # Default 4444: free next to MCM (6020/6021/6030/6040); 5777 is mavlink-server on BlueOS.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "4444")))
