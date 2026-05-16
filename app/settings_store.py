"""
Persistent UI / ops settings on the extension recordings mount (survives container restarts).
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Same bind mount BlueOS uses for recordings; survives reboot when host path is bound.
SETTINGS_DIR = "/app/recordings"
SETTINGS_PATH = os.path.join(SETTINGS_DIR, ".br_explorehd_dvr_settings.json")

# NeuralX upload defaults (see app/neuralx_uploader.py). The endpoint is the
# documented public test bucket from the NeuralX integration guide; treat as
# a default rather than a secret. `NEURALX_ENDPOINT` env var overrides it on
# first boot only — once persisted in the settings file the operator's UI
# value wins so that re-pointing at a staging endpoint survives container
# restarts without env-var gymnastics.
NEURALX_DEFAULT_ENDPOINT = os.environ.get(
    "NEURALX_ENDPOINT",
    "https://vv4ki4fa6b.execute-api.us-west-2.amazonaws.com/test-upload-url",
)
NEURALX_ALLOWED_CAM_IDS = ("01", "02", "03", "04")
NEURALX_DEFAULT_CAM_MAP: Dict[str, str] = {"0": "01", "1": "02", "2": "03", "3": "04"}
NEURALX_MAX_CONCURRENT_MIN = 1
NEURALX_MAX_CONCURRENT_MAX = 4
# node_id and the filename portion both have to satisfy the PDF's whitelist
# (`letters, digits, dots, underscores, hyphens — no spaces`). We use the same
# regex for both, plus a length cap on the node_id so it doesn't bloat every
# uploaded filename.
NEURALX_TOKEN_RE = re.compile(r"^[A-Za-z0-9._-]{1,40}$")

# `browser_tz_offset_minutes` is signed minutes EAST of UTC (so Hawaii / UTC-10
# is -600, Australia / UTC+10 is +600). It's the inverted form of JS's
# `Date.getTimezoneOffset()` which returns minutes WEST of UTC. We persist the
# last-known browser offset so segment timestamps and the session calendar-day
# directory stay in the operator's local time even when auto-record-on-boot
# starts before any browser has connected.
_DEFAULTS: Dict[str, Any] = {
    "auto_record_on_boot": True,
    "browser_tz_offset_minutes": None,
    "browser_tz_name": None,
    # Auto-download: when enabled, every open browser tab pulls a fresh zip
    # of newly-finalized recording segments every N minutes. Persisted here
    # (alongside auto_record_on_boot) so the toggle survives container/host
    # restarts via the BlueOS recordings bind mount.
    "auto_download_enabled": False,
    "auto_download_interval_minutes": 5,
    # NeuralX continuous-uploader settings. Disabled by default — opt-in only
    # because the documented endpoint is a public, unauthenticated test bucket.
    "neuralx_enabled": False,
    "neuralx_node_id": "",
    "neuralx_endpoint": NEURALX_DEFAULT_ENDPOINT,
    "neuralx_cam_map": dict(NEURALX_DEFAULT_CAM_MAP),
    "neuralx_max_concurrent": 1,
}

# Bounds for the periodic-download cadence. 1 minute floor: shorter than that
# and most ticks would carry no new closed segments (default segment length
# is 300s) so we'd just be hammering the operator with status .txt files.
# 1440 minutes (24h) ceiling is a sanity cap; the localStorage cursor still
# works for longer gaps when the tab is closed and reopened.
AUTO_DOWNLOAD_INTERVAL_MIN = 1
AUTO_DOWNLOAD_INTERVAL_MAX = 1440


def _validate_neuralx_cam_map(value: Any) -> Optional[Dict[str, str]]:
    """Return a normalized cam_map ({"0":"01", ...}) or None when invalid.

    Rules: keys are stringified cam indices "0".."3"; each value is in
    NEURALX_ALLOWED_CAM_IDS; values must be unique across the four slots so
    two cams on the same node can't accidentally share a server-side bucket.
    """
    if not isinstance(value, dict):
        return None
    out: Dict[str, str] = {}
    for k in ("0", "1", "2", "3"):
        v = value.get(k) or value.get(int(k))
        if not isinstance(v, str):
            return None
        if v not in NEURALX_ALLOWED_CAM_IDS:
            return None
        out[k] = v
    if len(set(out.values())) != len(out):
        return None
    return out


def _validate_neuralx_endpoint(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s.startswith(("https://", "http://")):
        return None
    return s


def _validate_neuralx_node_id(value: Any) -> Optional[str]:
    if value is None:
        return ""
    if not isinstance(value, str):
        return None
    s = value.strip()
    if s == "":
        return ""
    if not NEURALX_TOKEN_RE.match(s):
        return None
    return s


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
        if "browser_tz_offset_minutes" in raw:
            v = raw["browser_tz_offset_minutes"]
            if v is None:
                out["browser_tz_offset_minutes"] = None
            else:
                try:
                    out["browser_tz_offset_minutes"] = int(v)
                except (TypeError, ValueError):
                    out["browser_tz_offset_minutes"] = None
        if "browser_tz_name" in raw:
            v = raw["browser_tz_name"]
            out["browser_tz_name"] = str(v) if isinstance(v, str) and v else None
        if "auto_download_enabled" in raw:
            out["auto_download_enabled"] = bool(raw["auto_download_enabled"])
        if "auto_download_interval_minutes" in raw:
            v = raw["auto_download_interval_minutes"]
            try:
                iv = int(v)
            except (TypeError, ValueError):
                iv = None
            if iv is not None and AUTO_DOWNLOAD_INTERVAL_MIN <= iv <= AUTO_DOWNLOAD_INTERVAL_MAX:
                out["auto_download_interval_minutes"] = iv
        if "neuralx_enabled" in raw:
            out["neuralx_enabled"] = bool(raw["neuralx_enabled"])
        if "neuralx_node_id" in raw:
            v = _validate_neuralx_node_id(raw["neuralx_node_id"])
            if v is not None:
                out["neuralx_node_id"] = v
        if "neuralx_endpoint" in raw:
            v = _validate_neuralx_endpoint(raw["neuralx_endpoint"])
            if v is not None:
                out["neuralx_endpoint"] = v
        if "neuralx_cam_map" in raw:
            cm = _validate_neuralx_cam_map(raw["neuralx_cam_map"])
            if cm is not None:
                out["neuralx_cam_map"] = cm
        if "neuralx_max_concurrent" in raw:
            try:
                iv = int(raw["neuralx_max_concurrent"])
                iv = max(NEURALX_MAX_CONCURRENT_MIN, min(NEURALX_MAX_CONCURRENT_MAX, iv))
                out["neuralx_max_concurrent"] = iv
            except (TypeError, ValueError):
                pass
        # `neuralx_delete_below_free_mb` was a configurable threshold up
        # through 1.0.30. The deletion policy is now hardcoded inside
        # neuralx_uploader (50 GB free-space floor + 3-day age cap), so
        # we intentionally don't read the legacy key — leaving it on disk
        # is harmless, the next save_settings will not write it back.
    except Exception as e:
        logger.warning("Could not read %s: %s", SETTINGS_PATH, e)
    return out


def save_settings(updates: Dict[str, Any]) -> Dict[str, Any]:
    cur = load_settings()
    if "auto_record_on_boot" in updates:
        cur["auto_record_on_boot"] = bool(updates["auto_record_on_boot"])
    if "browser_tz_offset_minutes" in updates:
        v = updates["browser_tz_offset_minutes"]
        if v is None:
            cur["browser_tz_offset_minutes"] = None
        else:
            try:
                iv = int(v)
            except (TypeError, ValueError):
                iv = None
            # JS getTimezoneOffset gives values in [-840, 840]; clamp lightly so a
            # corrupted client can't poison the file with a giant or NaN value.
            if iv is not None and -24 * 60 <= iv <= 24 * 60:
                cur["browser_tz_offset_minutes"] = iv
            elif iv is None:
                cur["browser_tz_offset_minutes"] = None
    if "browser_tz_name" in updates:
        v = updates["browser_tz_name"]
        cur["browser_tz_name"] = str(v) if isinstance(v, str) and v else None
    if "auto_download_enabled" in updates:
        cur["auto_download_enabled"] = bool(updates["auto_download_enabled"])
    if "auto_download_interval_minutes" in updates:
        v = updates["auto_download_interval_minutes"]
        try:
            iv = int(v)
        except (TypeError, ValueError):
            iv = None
        if iv is not None:
            # Clamp rather than reject — the UI uses min/max on the input element
            # but a stale or out-of-band POST shouldn't be able to poison the file.
            iv = max(AUTO_DOWNLOAD_INTERVAL_MIN, min(AUTO_DOWNLOAD_INTERVAL_MAX, iv))
            cur["auto_download_interval_minutes"] = iv
    if "neuralx_enabled" in updates:
        cur["neuralx_enabled"] = bool(updates["neuralx_enabled"])
    if "neuralx_node_id" in updates:
        v = _validate_neuralx_node_id(updates["neuralx_node_id"])
        if v is None:
            raise ValueError(
                "neuralx_node_id must match [A-Za-z0-9._-]{1,40}"
            )
        cur["neuralx_node_id"] = v
    if "neuralx_endpoint" in updates:
        v = _validate_neuralx_endpoint(updates["neuralx_endpoint"])
        if v is None:
            raise ValueError("neuralx_endpoint must be an http(s) URL")
        cur["neuralx_endpoint"] = v
    if "neuralx_cam_map" in updates:
        cm = _validate_neuralx_cam_map(updates["neuralx_cam_map"])
        if cm is None:
            raise ValueError(
                "neuralx_cam_map must map cam indices 0..3 to unique values in "
                f"{NEURALX_ALLOWED_CAM_IDS}"
            )
        cur["neuralx_cam_map"] = cm
    if "neuralx_max_concurrent" in updates:
        try:
            iv = int(updates["neuralx_max_concurrent"])
        except (TypeError, ValueError):
            raise ValueError("neuralx_max_concurrent must be an integer")
        cur["neuralx_max_concurrent"] = max(
            NEURALX_MAX_CONCURRENT_MIN, min(NEURALX_MAX_CONCURRENT_MAX, iv)
        )
    # `neuralx_delete_below_free_mb` deliberately ignored on POST (now
    # hardcoded inside neuralx_uploader). Stale browsers may still send it
    # — we no-op silently rather than 400 the entire save.
    # Cross-field check: enabling the uploader requires a node_id so we can
    # disambiguate uploads from this Pi on the shared test bucket.
    if cur.get("neuralx_enabled") and not cur.get("neuralx_node_id"):
        raise ValueError(
            "neuralx_enabled requires neuralx_node_id to be set first"
        )
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


def get_browser_tz_offset_minutes() -> Optional[int]:
    """Returns the last-known browser TZ offset in minutes east of UTC, or None."""
    try:
        return load_settings().get("browser_tz_offset_minutes")
    except Exception:
        return None


def browser_local_datetime(epoch_seconds: Optional[float] = None) -> datetime:
    """Convert a wall-clock epoch (defaults to now) into the operator's browser local time.

    Falls back to the container's local time (typically UTC) if no browser has
    reported a TZ offset yet — that keeps a fresh-out-of-the-box install working
    on the very first boot, before any client has connected.
    """
    if epoch_seconds is None:
        epoch_seconds = time.time()
    offset = get_browser_tz_offset_minutes()
    if offset is None:
        return datetime.fromtimestamp(epoch_seconds)
    tz = timezone(timedelta(minutes=offset))
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).astimezone(tz)
