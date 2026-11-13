"""
Client for the DWE OS REST API, used to set exploreHD H.264 bitrate.

DWE OS's port is not fixed: BlueOS can hand the extension a different port on
every boot. Unless ``DWEOS_BASE`` pins it, the base URL is discovered from the
BlueOS helper's service list (``GET {HELPER_BASE}/v1.0/web_services``), cached,
and re-discovered whenever a request to the cached URL fails.

DWE OS owns the cameras' UVC extension-unit controls: it saves every control
per camera (keyed by V4L2 ``bus_info``) and re-applies them whenever a camera
appears, so setting bitrate anywhere else would be overwritten. We go through
its API instead:

- ``GET  /api/devices``                  -> devices with ``bus_info`` and
  ``controls[]`` (``control_id``, ``name``, ``value``)
- ``POST /api/devices/set_uvc_control``  -> ``{"bus_info", "control_id",
  "value"}``; bitrate value is Mbps (float). Applied live.

``set_uvc_control`` always answers ``{"success": true}``, even for an unknown
device, so every set is confirmed by reading the value back.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# Optional fixed override; when unset, DWE OS is located via the helper.
DWEOS_BASE = os.environ.get("DWEOS_BASE", "").rstrip("/")
HELPER_BASE = os.environ.get("BLUEOS_HELPER_BASE", "http://127.0.0.1:81").rstrip("/")
# DWE OS runs on the same host as the helper (loopback under NetworkMode: host).
DWEOS_HOST = urlparse(HELPER_BASE).hostname or "127.0.0.1"
TIMEOUT_S = 5.0

BITRATE_MIN_MBPS = 1.0
BITRATE_MAX_MBPS = 10.0
# DWE OS stores bitrate as integer bps; allow rounding on read-back.
_READBACK_TOLERANCE_MBPS = 0.05


class DweOsError(RuntimeError):
    pass


_base_lock = threading.Lock()
_cached_base: Optional[str] = None


def _is_dweos(service: Dict[str, Any]) -> bool:
    meta = service.get("metadata") or {}
    names = (meta.get("sanitized_name"), meta.get("name"), service.get("title"))
    return any(
        str(n).lower().replace("_", "").replace(" ", "").startswith("dweos")
        for n in names if n
    )


def _discover_base() -> str:
    try:
        r = requests.get(f"{HELPER_BASE}/v1.0/web_services", timeout=TIMEOUT_S)
        r.raise_for_status()
        services = r.json()
    except Exception as e:
        raise DweOsError(f"BlueOS helper unreachable at {HELPER_BASE}: {e}") from e
    for s in services if isinstance(services, list) else []:
        if isinstance(s, dict) and _is_dweos(s) and s.get("port"):
            return f"http://{DWEOS_HOST}:{int(s['port'])}"
    raise DweOsError("DWE OS is not running (not in the BlueOS helper's service list)")


def base_url(refresh: bool = False) -> str:
    """DWE OS base URL: the DWEOS_BASE override, else the cached discovery."""
    global _cached_base
    if DWEOS_BASE:
        return DWEOS_BASE
    with _base_lock:
        if _cached_base is None or refresh:
            _cached_base = _discover_base()
            logger.info("DWE OS discovered at %s", _cached_base)
        return _cached_base


def _request(method: str, path: str, **kwargs: Any) -> requests.Response:
    """Call DWE OS; on failure re-discover its port once and retry."""
    base = base_url()
    try:
        r = requests.request(method, f"{base}{path}", timeout=TIMEOUT_S, **kwargs)
        r.raise_for_status()
        return r
    except Exception as first:
        if DWEOS_BASE:
            raise DweOsError(f"DWE OS request to {base}{path} failed: {first}") from first
    fresh = base_url(refresh=True)
    try:
        r = requests.request(method, f"{fresh}{path}", timeout=TIMEOUT_S, **kwargs)
        r.raise_for_status()
        return r
    except Exception as e:
        raise DweOsError(f"DWE OS request to {fresh}{path} failed: {e}") from e


def list_devices() -> List[Dict[str, Any]]:
    r = _request("GET", "/api/devices")
    try:
        data = r.json()
    except ValueError as e:
        raise DweOsError(f"DWE OS /api/devices returned non-JSON: {e}") from e
    if isinstance(data, dict):
        data = list(data.values())
    if not isinstance(data, list):
        raise DweOsError(f"Unexpected /api/devices payload: {type(data).__name__}")
    return [d for d in data if isinstance(d, dict)]


def _mentions(value: Any, needle: str) -> bool:
    if isinstance(value, str):
        return value == needle
    if isinstance(value, dict):
        return any(_mentions(v, needle) for v in value.values())
    if isinstance(value, list):
        return any(_mentions(v, needle) for v in value)
    return False


def find_device(bus_info: Optional[str], device_path: Optional[str] = None,
                devices: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Match a camera by USB bus_info, falling back to its /dev/video path."""
    devices = list_devices() if devices is None else devices
    if bus_info:
        for d in devices:
            if d.get("bus_info") == bus_info:
                return d
    if device_path:
        for d in devices:
            if _mentions(d, device_path):
                return d
    raise DweOsError(
        f"DWE OS has no camera with bus_info={bus_info!r} "
        f"or device {device_path!r}"
    )


def bitrate_control(device: Dict[str, Any]) -> Dict[str, Any]:
    for c in device.get("controls") or []:
        name = str(c.get("name") or "").lower()
        if "bitrate" in name and "variable" not in name:
            return c
    raise DweOsError(
        f"Camera {device.get('bus_info')!r} has no Bitrate control in DWE OS"
    )


def get_bitrate(bus_info: Optional[str], device_path: Optional[str] = None) -> float:
    control = bitrate_control(find_device(bus_info, device_path))
    try:
        return float(control.get("value"))
    except (TypeError, ValueError) as e:
        raise DweOsError(f"Unreadable bitrate value {control.get('value')!r}") from e


def set_bitrate(bus_info: Optional[str], mbps: float,
                device_path: Optional[str] = None) -> float:
    """Set H.264 bitrate in Mbps; returns the value DWE OS reports afterwards."""
    if not BITRATE_MIN_MBPS <= mbps <= BITRATE_MAX_MBPS:
        raise ValueError(
            f"bitrate must be {BITRATE_MIN_MBPS:g}-{BITRATE_MAX_MBPS:g} Mbps"
        )
    device = find_device(bus_info, device_path)
    control = bitrate_control(device)
    body = {
        "bus_info": device.get("bus_info"),
        "control_id": control.get("control_id"),
        "value": float(mbps),
    }
    _request("POST", "/api/devices/set_uvc_control", json=body)
    logger.info("DWE OS: set bitrate %s -> %.2f Mbps", body["bus_info"], mbps)

    # DWE OS answers success unconditionally; trust only the read-back.
    actual = None
    for _ in range(3):
        actual = get_bitrate(body["bus_info"])
        if abs(actual - mbps) <= _READBACK_TOLERANCE_MBPS:
            return actual
        time.sleep(0.3)
    raise DweOsError(
        f"DWE OS reports {actual} Mbps after setting {mbps} Mbps"
    )
