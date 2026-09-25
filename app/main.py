"""
BR_exploreHD_DVR (qoocam branch) — relay direct QooCam H264 RTSP to a hardcoded
RTMP endpoint. No MCM / BlueOS Video Streams. Cloud-only build.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request, send_file

import cloud_relay
import local_preview
from boot_manager import run_boot_sequence
from settings_store import load_settings, save_settings
from stream_sources import list_direct_h264_rtsp_streams
from system_telemetry import get_all_telemetry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

VERSION = "1.1.2-qoocam"


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
    """Provider callback for cloud_relay: latest direct-source stream list."""
    with _state_lock:
        return list(streams_snapshot)


def _start_cloud_from_boot_streams(streams: List[Dict[str, Any]]) -> None:
    """Start local WebRTC preview and RTMP as soon as RTSP is reachable."""
    global streams_snapshot
    with _state_lock:
        streams_snapshot = list(streams)
    try:
        if streams:
            local_preview.start(streams[0]["rtsp_url"])
    except Exception:
        logger.exception("Local WebRTC preview failed to start")
    try:
        cloud_relay.configure(_current_streams_snapshot)
        cloud_relay.start_if_enabled()
        logger.info("Cloud relay started on direct RTSP (%d stream(s))", len(streams))
    except Exception:
        logger.exception("Cloud relay failed to start on direct RTSP")


def _boot_worker():
    global boot_stage, boot_error, streams_snapshot
    with _boot_lock:
        try:
            streams, err, stage = run_boot_sequence(
                "",
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
            "description": "Cloud RTMP relay for direct QooCam H264 RTSP (no MCM)",
            "icon": "mdi-cloud-upload",
            "company": "Blue Robotics",
            "version": VERSION,
            "webpage": "https://github.com/vshie/BR_exploreHD_DVR",
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
            "streams_warning": False,
            "ingest": "direct-qoocam",
            "telemetry": telem,
            "cloud": cloud_summary,
        }
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/streams", methods=["GET"])
def route_streams():
    """Configured direct RTSP sources; fall back to boot snapshot if probe fails."""
    streams: List[Dict[str, Any]] = []
    try:
        streams = list_direct_h264_rtsp_streams(require_reachable=False)
    except Exception as e:
        logger.warning("/streams: direct list failed: %s", e)
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
                "webrtc_page": None,
                "mcm_root": None,
                "running": s.get("running", False),
                "source": s.get("source", "direct"),
            }
        )
    return jsonify(out)


@app.route("/preview/status", methods=["GET"])
def route_preview_status():
    """Local MediaMTX/WHEP bridge details consumed by the Live tab."""
    try:
        local_preview.ensure_running()
        return jsonify({"preview": local_preview.status()})
    except Exception as e:
        logger.exception("preview status failed")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/live/ensure_streams", methods=["POST"])
def route_live_ensure_streams():
    """No MCM on this branch — report direct source reachability only."""
    streams = list_direct_h264_rtsp_streams(require_reachable=False)
    any_up = any(bool(s.get("running")) for s in streams)
    out = [
        {
            "index": i,
            "name": s["name"],
            "stream_id": s["stream_id"],
            "rtsp_url": s["rtsp_url"],
            "webrtc_page": None,
            "mcm_root": None,
            "running": s.get("running", False),
            "source": "direct",
        }
        for i, s in enumerate(streams)
    ]
    return jsonify(
        {
            "success": any_up,
            "kicked": False,
            "message": (
                "Direct QooCam RTSP (no MCM WebRTC). Cloud relay uses ffmpeg copy; "
                "preview in VLC with the rtsp_url."
            ),
            "streams": out,
        }
    )


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
    """Flip the persisted cloud-relay toggle and start/stop the relay."""
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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "4444")))
