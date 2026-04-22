"""
Persistent UI / ops settings on the extension recordings mount (survives container restarts).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Same bind mount BlueOS uses for recordings; survives reboot when host path is bound.
SETTINGS_DIR = "/app/recordings"
SETTINGS_PATH = os.path.join(SETTINGS_DIR, ".br_explorehd_dvr_settings.json")

_DEFAULTS: Dict[str, Any] = {
    "auto_record_on_boot": True,
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
        if "auto_record_on_boot" in raw:
            out["auto_record_on_boot"] = bool(raw["auto_record_on_boot"])
    except Exception as e:
        logger.warning("Could not read %s: %s", SETTINGS_PATH, e)
    return out


def save_settings(updates: Dict[str, Any]) -> Dict[str, Any]:
    cur = load_settings()
    if "auto_record_on_boot" in updates:
        cur["auto_record_on_boot"] = bool(updates["auto_record_on_boot"])
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
