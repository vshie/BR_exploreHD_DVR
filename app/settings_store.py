"""
Persistent settings for BR_exploreHD_DVR (cloud-only build).

The settings file lives at `/app/recordings/.br_explorehd_dvr_settings.json`.
BlueOS bind-mounts that path from the host (`/usr/blueos/extensions/br_explorehd_dvr`),
so the JSON survives container/image rebuilds without changing the extension's
Kraken permissions.

Only one key is persisted now: `cloud_relay_enabled`. Old recording-era keys
(`auto_record_on_boot`, `auto_download_*`, `browser_tz_*`, `neuralx_*`) are
ignored on load and dropped on the next save.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)

SETTINGS_DIR = "/app/recordings"
SETTINGS_PATH = os.path.join(SETTINGS_DIR, ".br_explorehd_dvr_settings.json")

_DEFAULTS: Dict[str, Any] = {
    # Cloud RTMP relay: when enabled (default ON), the extension spawns one
    # ffmpeg subprocess per MCM RTSP camera that copies the H.264 stream to
    # rtmp://35.83.28.160/live/bom_cam0N (see app/cloud_relay.py). The
    # destination URL is hardcoded; this toggle is the only knob.
    "cloud_relay_enabled": True,
}


def load_settings() -> Dict[str, Any]:
    out = dict(_DEFAULTS)
    if not os.path.isfile(SETTINGS_PATH):
        return out
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return out
        if "cloud_relay_enabled" in raw:
            out["cloud_relay_enabled"] = bool(raw["cloud_relay_enabled"])
    except Exception as e:
        logger.warning("Could not read %s: %s", SETTINGS_PATH, e)
    return out


def save_settings(updates: Dict[str, Any]) -> Dict[str, Any]:
    cur = load_settings()
    if "cloud_relay_enabled" in updates:
        cur["cloud_relay_enabled"] = bool(updates["cloud_relay_enabled"])
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
    return cur
