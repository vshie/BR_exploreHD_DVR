"""Local RTSP-to-WebRTC bridge for direct QooCam preview.

MediaMTX pulls the same in-camera RTSP source as the cloud relay and exposes
it through WHEP.  It remuxes H.264; there is no video transcode.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
from collections import deque
from typing import Any, Deque, Dict, Optional

logger = logging.getLogger(__name__)

MEDIAMTX_BIN = os.environ.get("MEDIAMTX_BIN", "/usr/local/bin/mediamtx")
MEDIAMTX_CONFIG = "/tmp/mediamtx-qoocam.yml"
WEBRTC_HTTP_PORT = int(os.environ.get("QOOCAM_WEBRTC_HTTP_PORT", "8889"))
WEBRTC_ICE_PORT = int(os.environ.get("QOOCAM_WEBRTC_ICE_PORT", "8189"))
WEBRTC_PATH = "qoocam"

_lock = threading.RLock()
_proc: Optional[subprocess.Popen] = None
_rtsp_url = ""
_last_error = ""
_recent_log: Deque[str] = deque(maxlen=20)


def _render_config(rtsp_url: str) -> str:
    source = json.dumps(rtsp_url)
    return f"""\
logLevel: info
logDestinations: [stdout]

api: false
metrics: false
pprof: false
playback: false
rtsp: false
rtmp: false
hls: false
srt: false

webrtc: true
webrtcAddress: :{WEBRTC_HTTP_PORT}
webrtcEncryption: false
webrtcAllowOrigins: ["*"]
webrtcLocalUDPAddress: :{WEBRTC_ICE_PORT}
webrtcLocalTCPAddress: :{WEBRTC_ICE_PORT}
webrtcIPsFromInterfaces: true

paths:
  {WEBRTC_PATH}:
    source: {source}
    sourceOnDemand: false
    rtspTransport: tcp
"""


def _log_reader(proc: subprocess.Popen) -> None:
    global _last_error
    if not proc.stdout:
        return
    try:
        for raw in iter(proc.stdout.readline, b""):
            if not raw:
                break
            line = raw.decode(errors="replace").strip()
            if not line:
                continue
            with _lock:
                _recent_log.append(line)
                low = line.lower()
                if "error" in low or "failed" in low:
                    _last_error = line[:500]
            logger.info("[mediamtx] %s", line)
    except Exception as exc:
        logger.debug("MediaMTX log reader stopped: %s", exc)


def start(rtsp_url: str) -> bool:
    """Start or retain the bridge for *rtsp_url*."""
    global _proc, _rtsp_url, _last_error
    with _lock:
        if _proc is not None and _proc.poll() is None and _rtsp_url == rtsp_url:
            return True
        stop()
        _rtsp_url = rtsp_url
        _last_error = ""
        _recent_log.clear()
        try:
            with open(MEDIAMTX_CONFIG, "w", encoding="utf-8") as f:
                f.write(_render_config(rtsp_url))
            _proc = subprocess.Popen(
                [MEDIAMTX_BIN, MEDIAMTX_CONFIG],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except Exception as exc:
            _proc = None
            _last_error = f"Could not start MediaMTX: {exc}"
            logger.exception(_last_error)
            return False
        threading.Thread(
            target=_log_reader,
            args=(_proc,),
            daemon=True,
            name="mediamtx-log",
        ).start()
        logger.info(
            "Local preview bridge: %s -> WHEP path %s (HTTP %d, ICE %d)",
            rtsp_url,
            WEBRTC_PATH,
            WEBRTC_HTTP_PORT,
            WEBRTC_ICE_PORT,
        )
        return True


def stop() -> None:
    global _proc
    with _lock:
        proc = _proc
        _proc = None
        if proc is None or proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        except Exception:
            logger.exception("Could not stop MediaMTX")


def ensure_running() -> bool:
    """Restart MediaMTX after an unexpected exit, retaining its source."""
    with _lock:
        url = _rtsp_url
        running = _proc is not None and _proc.poll() is None
    if running:
        return True
    return bool(url) and start(url)


def status() -> Dict[str, Any]:
    with _lock:
        running = _proc is not None and _proc.poll() is None
        exit_code = None if _proc is None else _proc.poll()
        return {
            "running": running,
            "exit_code": exit_code,
            "rtsp_url": _rtsp_url,
            "whep_path": f"/{WEBRTC_PATH}/whep",
            "http_port": WEBRTC_HTTP_PORT,
            "ice_port": WEBRTC_ICE_PORT,
            "last_error": _last_error,
            "recent_log": list(_recent_log)[-8:],
        }
