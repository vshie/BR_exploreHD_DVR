"""
USB external storage detection, mounting, and health monitoring for BR_exploreHD_DVR.
"""

import glob
import logging
import os
import subprocess
import threading
import time

logger = logging.getLogger(__name__)

USB_MOUNT_POINT = "/mnt/usb"
USB_MIN_FREE_GB = 5
DVR_DIR = "BR_exploreHD_DVR"
PROBE_INTERVAL_S = 30

_lock = threading.Lock()
_mounted = False
_device = None
_probe_thread = None
_stop_probe = threading.Event()


def _scan_usb_devices():
    """Return a list of partition device paths on removable block devices."""
    partitions = []
    for block in glob.glob("/sys/block/sd*"):
        try:
            with open(os.path.join(block, "removable"), "r") as f:
                if f.read().strip() != "1":
                    continue
        except Exception:
            continue
        dev_name = os.path.basename(block)
        for part in sorted(glob.glob(os.path.join(block, dev_name + "*"))):
            part_name = os.path.basename(part)
            dev_path = f"/dev/{part_name}"
            if os.path.exists(dev_path):
                partitions.append(dev_path)
        if not partitions:
            dev_path = f"/dev/{dev_name}"
            if os.path.exists(dev_path):
                partitions.append(dev_path)
    return partitions


def is_mounted():
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                if USB_MOUNT_POINT in line.split():
                    return True
    except Exception:
        pass
    return False


def try_mount():
    global _mounted, _device

    with _lock:
        if _mounted and is_mounted():
            return True

        partitions = _scan_usb_devices()
        if not partitions:
            _mounted = False
            _device = None
            return False

        os.makedirs(USB_MOUNT_POINT, exist_ok=True)

        if is_mounted():
            _mounted = True
            _device = _device or partitions[0]
            return True

        for dev in partitions:
            result = subprocess.run(
                ["mount", "-o", "rw", dev, USB_MOUNT_POINT],
                capture_output=True,
                timeout=10,
            )
            if result.returncode == 0:
                _mounted = True
                _device = dev
                logger.info(f"USB mounted: {dev} -> {USB_MOUNT_POINT}")
                return True
            logger.debug(f"mount {dev} failed: {result.stderr.decode(errors='replace').strip()}")

        _mounted = False
        _device = None
        return False


def unmount():
    global _mounted, _device
    with _lock:
        if is_mounted():
            subprocess.run(["umount", USB_MOUNT_POINT], capture_output=True, timeout=10)
            logger.info("USB unmounted")
        _mounted = False
        _device = None


def is_healthy():
    if not _mounted:
        return False
    try:
        os.statvfs(USB_MOUNT_POINT)
        return True
    except Exception:
        return False


def get_free_mb():
    if not _mounted:
        return None
    try:
        st = os.statvfs(USB_MOUNT_POINT)
        return round((st.f_bavail * st.f_frsize) / (1024 * 1024), 1)
    except Exception:
        return None


def is_usable():
    free = get_free_mb()
    if free is None:
        return False
    return free >= USB_MIN_FREE_GB * 1024


def get_recording_dir(subfolder_name):
    base = os.path.join(USB_MOUNT_POINT, DVR_DIR, subfolder_name)
    os.makedirs(base, exist_ok=True)
    return base


def get_status():
    mounted = _mounted and is_mounted()
    free = get_free_mb() if mounted else None
    return {
        "mounted": mounted,
        "device": _device,
        "free_mb": free,
        "usable": is_usable() if mounted else False,
        "mount_point": USB_MOUNT_POINT,
        "min_free_gb": USB_MIN_FREE_GB,
    }


def _probe_loop():
    while not _stop_probe.is_set():
        if not (_mounted and is_mounted()):
            try:
                try_mount()
            except Exception as e:
                logger.debug(f"USB probe error: {e}")
        _stop_probe.wait(PROBE_INTERVAL_S)


def start_probe():
    global _probe_thread
    if _probe_thread and _probe_thread.is_alive():
        return
    _stop_probe.clear()
    _probe_thread = threading.Thread(target=_probe_loop, daemon=True, name="usb-probe")
    _probe_thread.start()
    logger.info("USB probe thread started")


def stop_probe():
    _stop_probe.set()
    if _probe_thread and _probe_thread.is_alive():
        _probe_thread.join(timeout=5)
