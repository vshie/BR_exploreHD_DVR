"""
Client for the DWE OS REST API (default http://127.0.0.1:8000), used to set
exploreHD H.264 bitrate.

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
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

DWEOS_BASE = os.environ.get("DWEOS_BASE", "http://127.0.0.1:8000").rstrip("/")
TIMEOUT_S = 5.0

BITRATE_MIN_MBPS = 1.0
BITRATE_MAX_MBPS = 10.0
# DWE OS stores bitrate as integer bps; allow rounding on read-back.
_READBACK_TOLERANCE_MBPS = 0.05


class DweOsError(RuntimeError):
    pass


def list_devices() -> List[Dict[str, Any]]:
    try:
        r = requests.get(f"{DWEOS_BASE}/api/devices", timeout=TIMEOUT_S)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        raise DweOsError(f"DWE OS unreachable at {DWEOS_BASE}: {e}") from e
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
    try:
        r = requests.post(
            f"{DWEOS_BASE}/api/devices/set_uvc_control", json=body, timeout=TIMEOUT_S
        )
        r.raise_for_status()
    except Exception as e:
        raise DweOsError(f"DWE OS set_uvc_control failed: {e}") from e
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
