"""Local RTSP/HLS bridge for direct QooCam preview.

ffmpeg is the only client of the camera. It remuxes the original compressed
H.264 into MediaMTX, which serves RTSP to the cloud relay and fragmented
MP4/HLS to the browser. Nothing is decoded or re-encoded on the Pi.
"""

from __future__ import annotations

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
_ingest_proc: Optional[subprocess.Popen] = None
_ingest_thread: Optional[threading.Thread] = None
_ingest_stop = threading.Event()
_rtsp_url = ""
_last_error = ""
_recent_log: Deque[str] = deque(maxlen=40)


def local_rtsp_url() -> str:
    """Loopback copy of the camera stream. Cloud ffmpeg must use this."""
    return LOCAL_RTSP_URL


def _render_config(rtsp_url: str) -> str:
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
hlsVariant: fmp4
hlsSegmentDuration: 1s
hlsSegmentCount: 12
srt: false
moq: false

readTimeout: 30s
writeTimeout: 30s

webrtc: false

# Camera pull is owned by ffmpeg ({rtsp_url}), not by MediaMTX.
paths:
  {SOURCE_PATH}:
    source: publisher
    overridePublisher: true
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


def _ingest_command(rtsp_url: str) -> list[str]:
    """Copy the camera into MediaMTX. A silent socket is dropped after 2s."""
    return [
        "ffmpeg", "-hide_banner", "-loglevel", "warning",
        "-rtsp_transport", "tcp", "-stimeout", "2000000",
        "-i", rtsp_url,
        "-c", "copy",
        "-f", "rtsp", "-rtsp_transport", "tcp",
        f"rtsp://127.0.0.1:{LOCAL_RTSP_PORT}/{SOURCE_PATH}",
    ]


def _ingest_loop(rtsp_url: str, stop_event: threading.Event) -> None:
    global _ingest_proc
    while not stop_event.is_set():
        try:
            proc = subprocess.Popen(
                _ingest_command(rtsp_url),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except Exception as exc:
            logger.exception("Could not start QooCam ingest: %s", exc)
            if stop_event.wait(1.0):
                return
            continue
        with _lock:
            if stop_event.is_set():
                proc.terminate()
                return
            _ingest_proc = proc
        threading.Thread(
            target=_log_reader,
            args=(proc, "ingest"),
            daemon=True,
            name="qoocam-ingest-log",
        ).start()
        while proc.poll() is None and not stop_event.is_set():
            stop_event.wait(0.2)
        if stop_event.is_set():
            if proc.poll() is None:
                proc.terminate()
            return
        logger.warning("QooCam ingest exited (%s); restarting", proc.returncode)
        stop_event.wait(0.5)


def _stop_ingest() -> None:
    global _ingest_proc, _ingest_thread
    _ingest_stop.set()
    with _lock:
        proc = _ingest_proc
        thread = _ingest_thread
        _ingest_proc = None
        _ingest_thread = None
    if proc is not None and proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        except Exception:
            logger.exception("Could not stop QooCam ingest")
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=4)


def start(rtsp_url: str) -> bool:
    """Start or retain the bridge for *rtsp_url*."""
    global _proc, _rtsp_url, _last_error, _ingest_thread, _ingest_stop
    with _lock:
        mtx_up = _proc is not None and _proc.poll() is None and _rtsp_url == rtsp_url
        ingest_up = _ingest_thread is not None and _ingest_thread.is_alive()
    if mtx_up and ingest_up:
        return True
    stop()
    with _lock:
        _ingest_stop = threading.Event()
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
        _ingest_thread = threading.Thread(
            target=_ingest_loop,
            args=(rtsp_url, _ingest_stop),
            daemon=True,
            name="qoocam-ingest",
        )
        _ingest_thread.start()
        logger.info(
            "Local preview bridge: %s -> HLS /%s (HTTP %d)",
            rtsp_url,
            SOURCE_PATH,
            HLS_HTTP_PORT,
        )
    return True


def stop() -> None:
    global _proc
    _stop_ingest()
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
    """Restart the bridge after an unexpected exit, retaining its source."""
    with _lock:
        url = _rtsp_url
        mtx_up = _proc is not None and _proc.poll() is None
        ingest_up = _ingest_thread is not None and _ingest_thread.is_alive()
    if mtx_up and ingest_up:
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
