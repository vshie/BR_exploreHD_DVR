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
import dweos_client
from boot_manager import run_boot_sequence
from mcm_client import DEFAULT_MCM_BASE, fetch_streams_raw, kick_streams, list_h264_rtsp_streams
from settings_store import get_stream_prefs, load_settings, save_settings, set_stream_pref
from system_telemetry import get_all_telemetry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

VERSION = "1.0.43"

MCM_BASE = os.environ.get("MCM_BASE", DEFAULT_MCM_BASE).rstrip("/")

_boot_lock = threading.Lock()
_state_lock = threading.Lock()

boot_stage = "starting"
boot_error: Optional[str] = None
streams_snapshot: List[Dict[str, Any]] = []
# Last bitrate failure per stream key (DWE OS unreachable, read-back mismatch).
_bitrate_errors: Dict[str, str] = {}

# DWE OS may come up after us; keep trying to re-apply saved bitrates.
BITRATE_REAPPLY_ATTEMPTS = 12
BITRATE_REAPPLY_INTERVAL_S = 10.0


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
            threading.Thread(
                target=_reapply_saved_bitrates, daemon=True, name="bitrate-reapply"
            ).start()
        except Exception as e:
            logger.exception("Boot worker failed")
            with _state_lock:
                boot_error = str(e)
                boot_stage = "error"


def _reapply_saved_bitrates() -> None:
    """Make each camera match the bitrate last set through the API.

    DWE OS normally re-applies it itself; this covers a DWE OS reset or a
    swapped camera. Failures are recorded per stream and never block the relay.
    """
    pending = {
        e["stream_key"]: e
        for e in cloud_relay.stream_entries()
        if get_stream_prefs(e["stream_key"]).get("bitrate_mbps") is not None
    }
    for attempt in range(BITRATE_REAPPLY_ATTEMPTS):
        for key, entry in list(pending.items()):
            want = get_stream_prefs(key)["bitrate_mbps"]
            try:
                have = dweos_client.get_bitrate(entry.get("bus_info"), entry.get("device_path"))
                if abs(have - want) > 0.05:
                    logger.info("Re-applying %s bitrate %.2f Mbps (camera at %.2f)", key, want, have)
                    dweos_client.set_bitrate(entry.get("bus_info"), want, entry.get("device_path"))
                _bitrate_errors.pop(key, None)
                del pending[key]
            except Exception as e:
                _bitrate_errors[key] = str(e)
        if not pending:
            return
        time.sleep(BITRATE_REAPPLY_INTERVAL_S)
    for key, err in ((k, _bitrate_errors.get(k)) for k in pending):
        logger.error("Could not re-apply saved bitrate for %s: %s", key, err)


def _stream_object(entry: Dict[str, Any], devices: Optional[List[Dict[str, Any]]],
                   dweos_error: Optional[str]) -> Dict[str, Any]:
    key = entry["stream_key"]
    prefs = get_stream_prefs(key)
    relay = cloud_relay.relay_status(key)
    camera_mbps = None
    error = _bitrate_errors.get(key) or dweos_error
    if devices is not None:
        try:
            device = dweos_client.find_device(entry.get("bus_info"), entry.get("device_path"), devices)
            camera_mbps = float(dweos_client.bitrate_control(device).get("value"))
        except Exception as e:
            error = error or str(e)
    if relay is not None:
        relay_state = relay["state"]
    elif not prefs["enabled"]:
        relay_state = "disabled"
    else:
        relay_state = "off" if not cloud_relay.is_running() else "idle"
    return {
        "stream_key": key,
        "name": entry.get("name"),
        "stream_id": entry.get("stream_id"),
        "enabled": prefs["enabled"],
        "relay_state": relay_state,
        "relay_restarts": relay["restart_count"] if relay else 0,
        "relay_error": relay["last_error"] if relay else "",
        "bitrate_mbps_desired": prefs["bitrate_mbps"],
        "bitrate_mbps_camera": camera_mbps,
        "bus_info": entry.get("bus_info"),
        "device_path": entry.get("device_path"),
        "dweos_ok": devices is not None,
        "last_error": error or "",
    }


def _dweos_devices() -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    try:
        return dweos_client.list_devices(), None
    except Exception as e:
        return None, str(e)


def _find_entry(stream_key: str) -> Optional[Dict[str, Any]]:
    for e in cloud_relay.stream_entries():
        if e["stream_key"] == stream_key:
            return e
    return None


def _unknown_stream(stream_key: str):
    known = [e["stream_key"] for e in cloud_relay.stream_entries()]
    return jsonify({"success": False, "message": f"Unknown stream {stream_key!r}", "streams": known}), 404


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


# ---------------------------------------------------------------------------
# Remote control API for the cloud server (ZeroTier/Tailscale; no auth).
# Streams are addressed by RTMP stream key, e.g. bom_cam05.
# ---------------------------------------------------------------------------


@app.route("/api/streams", methods=["GET"])
def api_streams():
    devices, err = _dweos_devices()
    return jsonify({
        "success": True,
        "cloud_relay_enabled": bool(load_settings().get("cloud_relay_enabled", True)),
        "streams": [_stream_object(e, devices, err) for e in cloud_relay.stream_entries()],
    })


@app.route("/api/streams/<stream_key>", methods=["GET"])
def api_stream(stream_key: str):
    entry = _find_entry(stream_key)
    if entry is None:
        return _unknown_stream(stream_key)
    devices, err = _dweos_devices()
    return jsonify({"success": True, "stream": _stream_object(entry, devices, err)})


@app.route("/api/streams/<stream_key>/start", methods=["POST"])
def api_stream_start(stream_key: str):
    entry = _find_entry(stream_key)
    if entry is None:
        return _unknown_stream(stream_key)
    cloud_relay.start_stream(stream_key)
    devices, err = _dweos_devices()
    return jsonify({"success": True, "stream": _stream_object(entry, devices, err)})


@app.route("/api/streams/<stream_key>/stop", methods=["POST"])
def api_stream_stop(stream_key: str):
    entry = _find_entry(stream_key)
    if entry is None:
        return _unknown_stream(stream_key)
    cloud_relay.stop_stream(stream_key)
    devices, err = _dweos_devices()
    return jsonify({"success": True, "stream": _stream_object(entry, devices, err)})


@app.route("/api/streams/<stream_key>/bitrate", methods=["POST"])
def api_stream_bitrate(stream_key: str):
    entry = _find_entry(stream_key)
    if entry is None:
        return _unknown_stream(stream_key)
    data = request.get_json(silent=True) or {}
    mbps = data.get("mbps")
    if isinstance(mbps, bool) or not isinstance(mbps, (int, float)) or not (
        dweos_client.BITRATE_MIN_MBPS <= mbps <= dweos_client.BITRATE_MAX_MBPS
    ):
        return jsonify({
            "success": False,
            "message": (
                f"Body must be {{\"mbps\": number}} with "
                f"{dweos_client.BITRATE_MIN_MBPS:g}-{dweos_client.BITRATE_MAX_MBPS:g}"
            ),
        }), 400
    try:
        dweos_client.set_bitrate(entry.get("bus_info"), float(mbps), entry.get("device_path"))
    except dweos_client.DweOsError as e:
        _bitrate_errors[stream_key] = str(e)
        devices, err = _dweos_devices()
        return jsonify({
            "success": False,
            "message": str(e),
            "stream": _stream_object(entry, devices, err),
        }), 502
    set_stream_pref(stream_key, bitrate_mbps=float(mbps))
    _bitrate_errors.pop(stream_key, None)
    devices, err = _dweos_devices()
    return jsonify({"success": True, "stream": _stream_object(entry, devices, err)})


@app.route("/boot/retry", methods=["POST"])
def route_boot_retry():
    threading.Thread(target=_boot_worker, daemon=True, name="boot-retry").start()
    return jsonify({"success": True, "message": "Boot retry scheduled"})


if __name__ == "__main__":
    threading.Thread(target=_boot_worker, daemon=True, name="boot").start()
    # Default 4444: free next to MCM (6020/6021/6030/6040); 5777 is mavlink-server on BlueOS.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "4444")))
