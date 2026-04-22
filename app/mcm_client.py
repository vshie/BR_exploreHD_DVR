"""
Read-only client for MAVLink Camera Manager REST API (default http://127.0.0.1:6020).
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_MCM_BASE = os.environ.get("MCM_BASE", "http://127.0.0.1:6020").rstrip("/")


def _first_rtsp_url(endpoints: List[Any]) -> Optional[str]:
    for ep in endpoints or []:
        if isinstance(ep, str) and ep.lower().startswith("rtsp://"):
            return ep
    return None


def _is_h264_stream(stream_info: Dict[str, Any]) -> bool:
    cfg = stream_info.get("configuration") or {}
    if cfg.get("type") == "video":
        enc = (cfg.get("encode") or "").upper()
        return enc == "H264"
    return False


def parse_stream_status(item: Dict[str, Any], base: str = DEFAULT_MCM_BASE) -> Optional[Dict[str, Any]]:
    """Return normalized stream dict or None if unusable for H264 RTSP recording."""
    try:
        base = (base or DEFAULT_MCM_BASE).rstrip("/")
        sid = item.get("id")
        vas = item.get("video_and_stream") or {}
        name = vas.get("name") or "stream"
        si = vas.get("stream_information") or {}
        endpoints = si.get("endpoints") or []
        rtsp = _first_rtsp_url(endpoints)
        if not rtsp or not sid:
            return None
        if not _is_h264_stream(si):
            logger.debug(f"Skipping non-H264 stream {name!r}")
            return None
        return {
            "stream_id": str(sid),
            "name": name,
            "rtsp_url": rtsp,
            "webrtc_page": f"{base}/webrtc",
            "mcm_root": base,
            "running": bool(item.get("running")),
            "state": item.get("state"),
            "error": item.get("error"),
        }
    except Exception as e:
        logger.warning(f"Failed to parse stream entry: {e}")
        return None


def fetch_streams_raw(base: str = DEFAULT_MCM_BASE, timeout: float = 8.0) -> List[Dict[str, Any]]:
    url = f"{base}/streams"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected /streams payload: {type(data)}")
    return data


def list_h264_rtsp_streams(base: str = DEFAULT_MCM_BASE, timeout: float = 8.0) -> List[Dict[str, Any]]:
    raw = fetch_streams_raw(base=base, timeout=timeout)
    out: List[Dict[str, Any]] = []
    for item in raw:
        parsed = parse_stream_status(item, base=base)
        if parsed:
            out.append(parsed)
    out.sort(key=lambda s: s["name"].lower())
    return out


def kick_streams(base: str = DEFAULT_MCM_BASE, timeout: float = 5.0) -> bool:
    """Compat shim: MCM 0.2.4 exposes `POST /restart_streams` but the endpoint
    unconditionally returns `500 "Missing argument for restart_streams"` for
    every request shape we tried (query param `?use_persistent=true`, form
    body `use_persistent=true`, JSON body `{"use_persistent": true}`, empty
    body, etc.). Worse, in the broken state we currently observe, cycling
    pipelines is precisely what keeps MCM's RTSP fanout wedged. So this
    function is intentionally a no-op on current MCM: it reports "we tried"
    so callers continue, without actually poking MCM.

    Kept as a named function so we can re-enable it if a future MCM release
    ships a working `/restart_streams` endpoint and/or documents the argument.
    Returns True so callers fall through to their normal poll-and-list path.
    """
    logger.info(
        "MCM kick_streams: no-op (MCM 0.2.4 /restart_streams always 500s and "
        "cycling pipelines destabilizes the RTSP fanout)"
    )
    return True


def wait_for_streams(
    base: str = DEFAULT_MCM_BASE,
    poll_interval_s: float = 3.0,
    max_wait_s: float = 60.0,
) -> List[Dict[str, Any]]:
    """Poll MCM until first successful fetch or timeout. Returns list (may be empty)."""
    deadline = time.monotonic() + max_wait_s
    last_err: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            streams = list_h264_rtsp_streams(base=base)
            return streams
        except Exception as e:
            last_err = e
            logger.info(f"MCM not ready yet ({e}); retry in {poll_interval_s}s")
            time.sleep(poll_interval_s)
    if last_err:
        logger.error(f"MCM streams fetch failed after timeout: {last_err}")
    return []
