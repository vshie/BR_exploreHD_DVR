"""
Boot: CPU calm wait, zip prior sessions, USB mount, discover MCM streams (read-only).
"""

import logging
import os
import time
import uuid
import zipfile
from typing import Any, Dict, List, Optional, Tuple

import usb_storage
from mcm_client import wait_for_streams
from settings_store import browser_local_datetime

logger = logging.getLogger(__name__)

SEGMENT_SECONDS_DEFAULT = int(os.environ.get("SEGMENT_SECONDS", "300"))
LOADAVG_THRESHOLD = float(os.environ.get("BOOT_LOADAVG_MAX", "2.0"))
LOADAVG_STABLE_S = float(os.environ.get("BOOT_LOAD_STABLE_S", "5"))
BOOT_MAX_WAIT_S = float(os.environ.get("BOOT_MAX_WAIT_S", "90"))
BOOT_MIN_SLEEP_S = float(os.environ.get("BOOT_MIN_SLEEP_S", "20"))
MCM_POLL_S = float(os.environ.get("MCM_POLL_INTERVAL_S", "3"))
MCM_MAX_WAIT_S = float(os.environ.get("MCM_MAX_WAIT_S", "60"))


def _read_load1() -> Optional[float]:
    try:
        with open("/proc/loadavg", "r") as f:
            return float(f.read().split()[0])
    except Exception:
        return None


def wait_for_cpu_calm():
    """After BOOT_MIN_SLEEP_S, wait until 1m loadavg < threshold for LOADAVG_STABLE_S or BOOT_MAX_WAIT_S total."""
    time.sleep(BOOT_MIN_SLEEP_S)
    stable_start: Optional[float] = None
    deadline = time.monotonic() + BOOT_MAX_WAIT_S - BOOT_MIN_SLEEP_S
    while time.monotonic() < deadline:
        la = _read_load1()
        if la is not None and la < LOADAVG_THRESHOLD:
            now = time.monotonic()
            if stable_start is None:
                stable_start = now
            elif now - stable_start >= LOADAVG_STABLE_S:
                logger.info(f"CPU load calmed (loadavg={la})")
                return
        else:
            stable_start = None
        time.sleep(2)
    logger.warning("CPU calm wait timed out; proceeding anyway")


def _session_zip_path(session_dir: str) -> str:
    base = os.path.basename(session_dir.rstrip(os.sep))
    return os.path.join(session_dir, base + ".zip")


def zip_unfinished_sessions(recordings_base: str, skip_session_paths: Optional[set] = None):
    """
    For each .../YYYYMMDD/sessionId/ directory under recordings_base, if sessionId.zip
    is missing, build it from all files in that directory (excluding .zip).
    """
    skip_session_paths = skip_session_paths or set()
    if not os.path.isdir(recordings_base):
        return
    STORED = {".ts", ".mp4", ".jpg", ".jpeg", ".png"}
    for date_name in sorted(os.listdir(recordings_base)):
        date_path = os.path.join(recordings_base, date_name)
        if not os.path.isdir(date_path):
            continue
        if len(date_name) != 8 or not date_name.isdigit():
            continue
        for sess in sorted(os.listdir(date_path)):
            sess_path = os.path.join(date_path, sess)
            if not os.path.isdir(sess_path):
                continue
            if os.path.abspath(sess_path) in skip_session_paths:
                continue
            zip_path = _session_zip_path(sess_path)
            if os.path.exists(zip_path):
                continue
            files = []
            for f in sorted(os.listdir(sess_path)):
                fp = os.path.join(sess_path, f)
                if os.path.isfile(fp) and not f.endswith(".zip"):
                    files.append((f, fp, os.path.getsize(fp)))
            if not files:
                continue

            tmp = zip_path + ".tmp"
            logger.info(f"Zipping prior session {sess_path} ({len(files)} files)")
            try:
                with zipfile.ZipFile(tmp, "w") as zf:
                    for fname, fpath, _ in files:
                        ext = os.path.splitext(fname)[1].lower()
                        method = zipfile.ZIP_STORED if ext in STORED else zipfile.ZIP_DEFLATED
                        zf.write(fpath, fname, compress_type=method)
                os.replace(tmp, zip_path)
                logger.info(f"Session zip complete: {zip_path}")
            except Exception as e:
                logger.error(f"Session zip failed for {sess_path}: {e}")
                for p in (tmp, zip_path):
                    if os.path.exists(p):
                        try:
                            os.remove(p)
                        except OSError:
                            pass


def choose_recording_base() -> str:
    """Prefer USB BR_exploreHD_DVR tree when usable, else local extension dir."""
    local = "/app/recordings"
    if usb_storage.is_usable():
        base = os.path.join(usb_storage.USB_MOUNT_POINT, usb_storage.DVR_DIR)
        os.makedirs(base, exist_ok=True)
        logger.info(f"Recording base (USB): {base}")
        return base
    os.makedirs(local, exist_ok=True)
    logger.info(f"Recording base (local SD): {local}")
    return local


def run_boot_sequence(
    mcm_base: str,
) -> Tuple[str, Optional[str], str, List[Dict[str, Any]], Optional[str], str]:
    """
    Returns (recording_base, session_root, session_id, streams, boot_error, boot_stage).
    session_root is None if boot_error (no MCM streams).
    """
    boot_stage = "cpu_wait"
    wait_for_cpu_calm()
    boot_stage = "zip_prior"
    local = "/app/recordings"
    os.makedirs(local, exist_ok=True)
    zip_unfinished_sessions(local)
    usb_storage.try_mount()
    usb_storage.start_probe()
    if usb_storage.is_mounted():
        usb_root = os.path.join(usb_storage.USB_MOUNT_POINT, usb_storage.DVR_DIR)
        os.makedirs(usb_root, exist_ok=True)
        zip_unfinished_sessions(usb_root)

    boot_stage = "mcm_wait"
    streams = wait_for_streams(base=mcm_base, poll_interval_s=MCM_POLL_S, max_wait_s=MCM_MAX_WAIT_S)
    boot_error: Optional[str] = None
    if not streams:
        boot_error = (
            "No H264 RTSP streams from MAVLink Camera Manager. "
            "Configure streams in BlueOS (Video Streams / MCM, port 6020)."
        )
        boot_stage = "mcm_error"
        recording_base = choose_recording_base()
        session_id = str(uuid.uuid4())
        return recording_base, None, session_id, [], boot_error, boot_stage

    if len(streams) < 4:
        logger.warning(f"Only {len(streams)} H264 RTSP stream(s) from MCM (expected up to 4)")

    boot_stage = "ready"
    recording_base = choose_recording_base()
    session_id = str(uuid.uuid4())
    # Use the operator's last-known browser TZ for the calendar-day directory
    # so it matches the segment timestamps written under it. Falls back to the
    # container's local time (UTC) if no browser has reported a TZ yet.
    today = browser_local_datetime().strftime("%Y%m%d")
    session_path = os.path.join(recording_base, today, session_id)
    os.makedirs(session_path, exist_ok=True)
    logger.info(f"New session directory: {session_path}")
    return recording_base, session_path, session_id, streams, boot_error, boot_stage
