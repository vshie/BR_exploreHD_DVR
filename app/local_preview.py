"""Local RTSP/HLS bridge for direct QooCam preview.

MediaMTX is the only client of the camera. It remuxes the original compressed
H.264 to RTSP for the cloud relay and fragmented MP4/HLS for the browser.
Nothing is decoded or re-encoded on the Pi.
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
HLS_HTTP_PORT = int(os.environ.get("QOOCAM_HLS_HTTP_PORT", "8888"))
LOCAL_RTSP_PORT = int(os.environ.get("QOOCAM_LOCAL_RTSP_PORT", "8554"))
SOURCE_PATH = "qoocam"
LOCAL_RTSP_URL = f"rtsp://127.0.0.1:{LOCAL_RTSP_PORT}/{SOURCE_PATH}"

_lock = threading.RLock()
_proc: Optional[subprocess.Popen] = None
_rtsp_url = ""
_last_error = ""
_recent_log: Deque[str] = deque(maxlen=40)


def local_rtsp_url() -> str:
    """Loopback copy of the camera stream. Cloud ffmpeg must use this."""
    return LOCAL_RTSP_URL


def _render_config(rtsp_url: str) -> str:
    source = json.dumps(rtsp_url)
    return f"""\
logLevel: info
logDestinations: [stdout]

api: false
metrics: false
pprof: false
playback: false

rtsp: true
rtspAddress: 127.0.0.1:{LOCAL_RTSP_PORT}
rtspTransports: [tcp]
rtmp: false
hls: true
hlsAddress: :{HLS_HTTP_PORT}
hlsEncryption: false
hlsAllowOrigins: ["*"]
hlsAlwaysRemux: true
hlsVariant: lowLatency
srt: false
moq: false

readTimeout: 30s
writeTimeout: 30s

webrtc: false

paths:
  {SOURCE_PATH}:
    source: {source}
    sourceOnDemand: false
    rtspTransport: tcp
"""


def _note(line: str) -> None:
    global _last_error
    with _lock:
        _recent_log.append(line)
        low = line.lower()
        if " war " not in low and ("error" in low or "failed" in low):
            _last_error = line[:500]


def _log_reader(proc: subprocess.Popen, prefix: str) -> None:
    if not proc.stdout:
        return
    try:
        for raw in iter(proc.stdout.readline, b""):
            if not raw:
                break
            line = raw.decode(errors="replace").strip()
            if not line:
                continue
            _note(f"[{prefix}] {line}")
            logger.info("[%s] %s", prefix, line)
    except Exception as exc:
        logger.debug("%s log reader stopped: %s", prefix, exc)


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
            args=(_proc, "mediamtx"),
            daemon=True,
            name="mediamtx-log",
        ).start()
        logger.info(
            "Local preview bridge: %s -> HLS /%s (HTTP %d)",
            rtsp_url,
            SOURCE_PATH,
            HLS_HTTP_PORT,
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
            "local_rtsp_url": LOCAL_RTSP_URL,
            "hls_path": f"/{SOURCE_PATH}/index.m3u8",
            "http_port": HLS_HTTP_PORT,
            "transcoding": False,
            "last_error": _last_error,
            "recent_log": list(_recent_log)[-8:],
        }
