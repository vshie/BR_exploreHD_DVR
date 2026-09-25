"""Configure the QooCam RTSP encoder through its OSC API.

The Enterprise camera's auto-live mode starts an 8K/60 Mbps RTSP preview.
That exceeds both the requested uplink budget and common browser H.264 decode
limits.  Recreate the RTSP preview at 3840x1920, H.264, 30 fps and 20 Mbps.
"""

from __future__ import annotations

import http.client
import json
import logging
import socket
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

TARGET_WIDTH = 3840
TARGET_HEIGHT = 1920
TARGET_FPS = 30
TARGET_BITRATE_MBPS = 20
TARGET_RESOLUTION = f"{TARGET_HEIGHT}*{TARGET_WIDTH}"


def _osc(host: str, name: str, parameters: Dict[str, Any] | None = None,
         timeout: float = 8.0) -> Dict[str, Any]:
    body: Dict[str, Any] = {"name": name}
    if parameters is not None:
        body["parameters"] = parameters
    payload = json.dumps(body).encode("utf-8")
    conn = http.client.HTTPConnection(host, 80, timeout=timeout)
    try:
        conn.request(
            "POST",
            "/osc/commands/execute",
            body=payload,
            headers={"Content-Type": "application/json"},
        )
        response = conn.getresponse()
        raw = response.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))
    finally:
        conn.close()


def _rtsp_open(host: str, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, 8554), timeout=timeout):
            return True
    except OSError:
        return False


def configure_rtsp_preview(host: str, wait_s: float = 30.0) -> None:
    """Restart the camera's RTSP preview at 4K/20 Mbps.

    ``camera.stopCapture`` does not apply to Live Pro RTSP mode.  The private
    OSC pair used by Kandao's own application is
    ``_stopRtspLivePreview`` / ``_startRtspLivePreview``.  Bitrate is supplied
    in Kbit/s as a string and the resolution is height*width.
    """
    logger.info(
        "Configuring QooCam RTSP: %dx%d H.264 @ %dfps, %d Mbps",
        TARGET_WIDTH,
        TARGET_HEIGHT,
        TARGET_FPS,
        TARGET_BITRATE_MBPS,
    )

    stopped = _osc(host, "camera._stopRtspLivePreview", {})
    if stopped.get("state") not in ("done", None):
        raise RuntimeError(f"QooCam RTSP stop failed: {stopped}")

    options = {
        "_resolution": TARGET_RESOLUTION,
        "_bitRate": str(TARGET_BITRATE_MBPS * 1000),
    }
    try:
        started = _osc(
            host,
            "camera._startRtspLivePreview",
            {"options": options},
            timeout=12.0,
        )
        if started.get("state") not in ("done", None):
            raise RuntimeError(f"QooCam RTSP start failed: {started}")
    except (TimeoutError, socket.timeout):
        # This command opens a streaming OSC response on some firmware builds.
        # Verify the resulting RTSP listener below instead of requiring EOF.
        logger.info("QooCam OSC start response remains open; verifying RTSP")

    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        if _rtsp_open(host):
            logger.info("QooCam 4K/20 Mbps RTSP preview is listening")
            return
        time.sleep(1.0)
    raise RuntimeError("QooCam RTSP did not return after encoder configuration")
