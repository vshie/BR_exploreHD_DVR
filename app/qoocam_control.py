"""Configure the QooCam RTSP encoder through its OSC API.

The Enterprise camera's auto-live mode starts an 8K/60 Mbps RTSP preview.
That exceeds both the requested uplink budget and common browser H.264 decode
limits.  Recreate the RTSP preview at 3840x1920, H.264, 30 fps and 15 Mbps.
"""

from __future__ import annotations

import http.client
import json
import logging
import socket
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

TARGET_WIDTH = 3840
TARGET_HEIGHT = 1920
TARGET_FPS = 30
TARGET_BITRATE_MBPS = 15
TARGET_RESOLUTION = f"{TARGET_HEIGHT}*{TARGET_WIDTH}"
RTSP_PORT = 8554

POWER_CYCLE_HINT = (
    "Power cycle the QooCam with its circuit breaker in the topside "
    "electrical box: switch it off, wait 10 seconds, switch it back on. "
    "The camera takes about a minute to boot into Live; video resumes "
    "automatically once it does."
)


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


def rtsp_video_ready(host: str, port: int = RTSP_PORT, path: str = "/",
                     timeout: float = 6.0) -> bool:
    """True only when the camera answers DESCRIBE with a video track.

    A TCP connect is not enough: a stalled encoder keeps 8554 listening and
    answers OPTIONS, but DESCRIBE never returns. Call this only while no other
    client (MediaMTX) holds the camera; a second session makes it drop both.
    """
    url = f"rtsp://{host}:{port}{path}"
    request = (
        f"DESCRIBE {url} RTSP/1.0\r\n"
        "CSeq: 1\r\n"
        "Accept: application/sdp\r\n"
        "User-Agent: BR_exploreHD_DVR\r\n\r\n"
    ).encode("ascii")
    deadline = time.monotonic() + timeout
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.sendall(request)
            data = b""
            body_len: Optional[int] = None
            while time.monotonic() < deadline:
                sock.settimeout(max(0.1, deadline - time.monotonic()))
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
                head, sep, body = data.partition(b"\r\n\r\n")
                if not sep:
                    continue
                if body_len is None:
                    body_len = 0
                    for line in head.split(b"\r\n")[1:]:
                        key, _, value = line.partition(b":")
                        if key.strip().lower() == b"content-length":
                            body_len = int(value.strip() or 0)
                if len(body) >= body_len:
                    break
    except (OSError, ValueError) as exc:
        logger.debug("RTSP DESCRIBE %s failed: %s", url, exc)
        return False
    head, _, body = data.partition(b"\r\n\r\n")
    status = head.split(b"\r\n", 1)[0].split()
    ok = len(status) >= 2 and status[1] == b"200" and b"m=video" in body
    if not ok:
        logger.info("QooCam RTSP DESCRIBE returned no video: %r", head[:120])
    return ok


def configure_rtsp_preview(host: str, wait_s: float = 30.0) -> None:
    """Restart the camera's RTSP preview at 4K/15 Mbps.

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
        if rtsp_video_ready(host, timeout=min(6.0, max(1.0, deadline - time.monotonic()))):
            logger.info("QooCam 4K/15 Mbps RTSP preview is serving video")
            return
        time.sleep(1.0)
    raise RuntimeError("QooCam RTSP served no video after encoder configuration")
