"""
Direct RTSP sources for the qoocam branch (no MCM / BlueOS Video Streams).

The QooCam has a fixed address on the vehicle network. ``QOOCAM_RTSP_URL``
overrides it.

The stream ``name`` must embed a camera number so cloud_relay maps it to
``bom_camNN`` (e.g. "QooCam 9" → bom_cam09).
"""

from __future__ import annotations

import logging
import os
import socket
import time
from typing import Any, Dict, List
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DEFAULT_QOOCAM_STREAM_NAME = "QooCam 9"
DEFAULT_QOOCAM_RTSP_URL = "rtsp://192.168.84.169:8554/"


def _rtsp_tcp_open(rtsp_url: str, timeout: float = 2.0) -> bool:
    try:
        parsed = urlparse(rtsp_url)
        host = parsed.hostname
        if not host:
            return False
        port = parsed.port or 554
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError as e:
        logger.debug("RTSP probe failed for %s: %s", rtsp_url, e)
        return False


def qoocam_rtsp_url() -> str:
    raw = (os.environ.get("QOOCAM_RTSP_URL") or "").strip()
    return raw or DEFAULT_QOOCAM_RTSP_URL


def _configured_sources() -> List[Dict[str, str]]:
    names = (os.environ.get("QOOCAM_STREAM_NAME") or DEFAULT_QOOCAM_STREAM_NAME).strip()
    name_list = [n.strip() for n in names.split(",") if n.strip()]
    name = name_list[0] if name_list else DEFAULT_QOOCAM_STREAM_NAME
    return [{"name": name, "rtsp_url": qoocam_rtsp_url()}]


def list_direct_h264_rtsp_streams(require_reachable: bool = True) -> List[Dict[str, Any]]:
    """Return normalized stream dicts compatible with cloud_relay / /streams API."""
    out: List[Dict[str, Any]] = []
    for i, src in enumerate(_configured_sources()):
        url = src["rtsp_url"]
        reachable = _rtsp_tcp_open(url)
        if require_reachable and not reachable:
            continue
        out.append(
            {
                "stream_id": f"qoocam-{i}",
                "name": src["name"],
                "rtsp_url": url,
                "webrtc_page": None,
                "mcm_root": None,
                "running": reachable,
                "source": "direct",
            }
        )
    return out


def wait_for_direct_streams(
    poll_interval_s: float = 2.0,
    max_wait_s: float = 60.0,
) -> List[Dict[str, Any]]:
    """Poll until the QooCam RTSP is reachable, or timeout."""
    deadline = time.monotonic() + max_wait_s
    last: List[Dict[str, Any]] = []
    while time.monotonic() < deadline:
        last = list_direct_h264_rtsp_streams(require_reachable=True)
        if last:
            return last
        logger.info(
            "Waiting for QooCam RTSP at %s; retry in %.1fs",
            qoocam_rtsp_url(),
            poll_interval_s,
        )
        time.sleep(poll_interval_s)
    return last
