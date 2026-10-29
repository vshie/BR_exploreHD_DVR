"""
Persistent settings for BR_exploreHD_DVR (cloud-only build).

The settings file lives at `/app/recordings/.br_explorehd_dvr_settings.json`.
BlueOS bind-mounts that path from the host (`/usr/blueos/extensions/br_explorehd_dvr`),
so the JSON survives container/image rebuilds without changing the extension's
Kraken permissions.

Persisted keys:
- `cloud_relay_enabled`: global cloud relay toggle (default ON).
- `streams`: per-stream overrides keyed by RTMP stream key (`bom_camNN`),
  each `{"enabled": bool, "bitrate_mbps": float|null}`. A stream with no
  entry uploads and keeps the camera's bitrate. Set through the remote
  control API (`/api/streams/...`), sticky until changed.

Old recording-era keys (`auto_record_on_boot`, `auto_download_*`,
`browser_tz_*`, `neuralx_*`) are ignored on load and dropped on the next save.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

SETTINGS_DIR = os.environ.get("DVR_SETTINGS_DIR", "/app/recordings")
SETTINGS_PATH = os.path.join(SETTINGS_DIR, ".br_explorehd_dvr_settings.json")

_DEFAULTS: Dict[str, Any] = {
    # Cloud RTMP relay: when enabled (default ON), the extension spawns one
    # ffmpeg subprocess per MCM RTSP camera that copies the H.264 stream to
    # rtmp://35.83.28.160/live/bom_cam0N (see app/cloud_relay.py). The
    # destination URL is hardcoded; this toggle is the only knob.
    "cloud_relay_enabled": True,
    "streams": {},
}

_STREAM_DEFAULTS: Dict[str, Any] = {"enabled": True, "bitrate_mbps": None}

# Serialises read-modify-write so API calls and the UI toggle can't lose
# each other's updates.
_lock = threading.RLock()


def _clean_stream_prefs(raw: Any) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for key, prefs in raw.items():
        if not isinstance(key, str) or not isinstance(prefs, dict):
            continue
        entry = dict(_STREAM_DEFAULTS)
        if "enabled" in prefs:
            entry["enabled"] = bool(prefs["enabled"])
        rate = prefs.get("bitrate_mbps")
        if isinstance(rate, (int, float)) and not isinstance(rate, bool):
            entry["bitrate_mbps"] = float(rate)
        out[key] = entry
    return out


def load_settings() -> Dict[str, Any]:
    out = dict(_DEFAULTS)
    out["streams"] = {}
    if not os.path.isfile(SETTINGS_PATH):
        return out
    try:
        with _lock, open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return out
        if "cloud_relay_enabled" in raw:
            out["cloud_relay_enabled"] = bool(raw["cloud_relay_enabled"])
        out["streams"] = _clean_stream_prefs(raw.get("streams"))
    except Exception as e:
        logger.warning("Could not read %s: %s", SETTINGS_PATH, e)
    return out


def save_settings(updates: Dict[str, Any]) -> Dict[str, Any]:
    with _lock:
        cur = load_settings()
        if "cloud_relay_enabled" in updates:
            cur["cloud_relay_enabled"] = bool(updates["cloud_relay_enabled"])
        if "streams" in updates:
            cur["streams"] = _clean_stream_prefs(updates["streams"])
        _write(cur)
    return cur


def get_stream_prefs(stream_key: str) -> Dict[str, Any]:
    """Overrides for one stream, with defaults filled in."""
    entry = dict(_STREAM_DEFAULTS)
    entry.update(load_settings()["streams"].get(stream_key, {}))
    return entry


def set_stream_pref(stream_key: str, enabled: Optional[bool] = None,
                    bitrate_mbps: Optional[float] = None) -> Dict[str, Any]:
    """Persist one stream's override; fields left as None are unchanged."""
    with _lock:
        cur = load_settings()
        entry = dict(_STREAM_DEFAULTS)
        entry.update(cur["streams"].get(stream_key, {}))
        if enabled is not None:
            entry["enabled"] = bool(enabled)
        if bitrate_mbps is not None:
            entry["bitrate_mbps"] = float(bitrate_mbps)
        cur["streams"][stream_key] = entry
        _write(cur)
    return entry


def _write(cur: Dict[str, Any]) -> None:
    try:
        os.makedirs(SETTINGS_DIR, exist_ok=True)
        tmp = SETTINGS_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cur, f, indent=2, sort_keys=True)
            f.write("\n")
        os.replace(tmp, SETTINGS_PATH)
    except Exception as e:
        logger.exception("Failed to write settings: %s", e)
        raise
