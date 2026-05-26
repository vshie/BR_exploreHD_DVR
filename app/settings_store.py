"""
Persistent UI / ops settings on the extension recordings mount (survives container restarts).

The settings file lives at `/app/recordings/.br_explorehd_dvr_settings.json`,
which BlueOS bind-mounts from the host (`/usr/blueos/extensions/br_explorehd_dvr`),
so the values here outlive the container and any image rebuild — i.e. they are
"stored external to docker" by the bind mount.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Same bind mount BlueOS uses for recordings; survives reboot when host path is bound.
SETTINGS_DIR = "/app/recordings"
SETTINGS_PATH = os.path.join(SETTINGS_DIR, ".br_explorehd_dvr_settings.json")

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
    # Cloud RTMP relay: when enabled (default ON), the extension spawns one
    # ffmpeg subprocess per MCM RTSP camera that copies the H.264 stream to
    # rtmp://35.83.28.160/live/bom_cam0N (see app/cloud_relay.py). The
    # destination URL is hardcoded; this toggle is the only knob.
    "cloud_relay_enabled": True,
}

# Bounds for the periodic-download cadence. 1 minute floor: shorter than that
# and most ticks would carry no new closed segments (default segment length
# is 300s) so we'd just be hammering the operator with status .txt files.
# 1440 minutes (24h) ceiling is a sanity cap; the localStorage cursor still
# works for longer gaps when the tab is closed and reopened.
AUTO_DOWNLOAD_INTERVAL_MIN = 1
AUTO_DOWNLOAD_INTERVAL_MAX = 1440


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
        if "cloud_relay_enabled" in raw:
            out["cloud_relay_enabled"] = bool(raw["cloud_relay_enabled"])
        # Legacy `neuralx_*` keys (1.0.31 and earlier) are intentionally
        # ignored. The next save_settings call will not write them back,
        # so the settings file naturally drains them on first edit.
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
