"""
External block storage (USB stick, USB M.2 enclosure, or NVMe) detection, mount, and health checks.
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

# Optional: force a single partition (e.g. M.2 USB enclosure that reports removable=0 and is not auto-detected)
# Example: EXTERNAL_STORAGE_DEVICE=/dev/sda1
_FORCED_DEVICE = os.environ.get("EXTERNAL_STORAGE_DEVICE", "").strip() or None

_lock = threading.Lock()
_mounted = False
_device = None
_probe_thread = None
_stop_probe = threading.Event()


def _system_mount_devices():
    """Device nodes used for OS root/boot (do not treat as external DVR media)."""
    out = set()
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2:
                    continue
                dev, mnt = parts[0], parts[1]
                if not dev.startswith("/dev/"):
                    continue
                if mnt == "/" or mnt.startswith("/boot"):
                    out.add(dev)
    except Exception:
        pass
    return out


def _block_partitions(block_path: str, dev_name: str) -> list:
    """Partition device paths for a /sys/block/<dev> entry (may be empty)."""
    found = []
    for part in sorted(glob.glob(os.path.join(block_path, dev_name + "*"))):
        part_name = os.path.basename(part)
        if part_name == dev_name:
            continue
        dev_path = f"/dev/{part_name}"
        if os.path.exists(dev_path):
            found.append(dev_path)
    if not found:
        dev_path = f"/dev/{dev_name}"
        if os.path.exists(dev_path):
            found.append(dev_path)
    return found


def _scan_external_partitions():
    """
    Candidate partitions for DVR external storage.

    - USB flash / USB NVMe enclosures usually appear as sd*. Many SSDs report removable=0;
      we include sd* that are not mounted as / or /boot*.
    - Native NVMe (e.g. M.2 HAT) appears as nvme*n*p*; same exclusion rule.
    - exFAT / FAT32 are supported at mount time (see try_mount).
    """
    system_devs = _system_mount_devices()
    partitions: list = []

    if _FORCED_DEVICE and os.path.exists(_FORCED_DEVICE):
        return [_FORCED_DEVICE]

    preferred_sd, other_sd = [], []
    for block in sorted(glob.glob("/sys/block/sd*")):
        dev_name = os.path.basename(block)
        try:
            with open(os.path.join(block, "removable"), "r") as f:
                rem = f.read().strip()
        except Exception:
            rem = "0"
        bucket = preferred_sd if rem == "1" else other_sd
        for p in _block_partitions(block, dev_name):
            if p not in system_devs:
                bucket.append(p)
    partitions.extend(preferred_sd)
    partitions.extend(other_sd)

    for block in sorted(glob.glob("/sys/block/nvme*")):
        dev_name = os.path.basename(block)
        if not dev_name.startswith("nvme") or "p" in dev_name:
            continue
        for p in _block_partitions(block, dev_name):
            if p in system_devs:
                continue
            partitions.append(p)

    seen = set()
    ordered = []
    for p in partitions:
        if p not in seen:
            seen.add(p)
            ordered.append(p)
    return ordered


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

        partitions = _scan_external_partitions()
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
            mount_cmds = [
                ["mount", "-o", "rw", dev, USB_MOUNT_POINT],
                ["mount", "-t", "exfat", "-o", "rw", dev, USB_MOUNT_POINT],
                ["mount", "-t", "vfat", "-o", "rw", dev, USB_MOUNT_POINT],
            ]
            for cmd in mount_cmds:
                result = subprocess.run(cmd, capture_output=True, timeout=15)
                if result.returncode == 0:
                    _mounted = True
                    _device = dev
                    logger.info("External storage mounted: %s -> %s (%s)", dev, USB_MOUNT_POINT, " ".join(cmd))
                    return True
            logger.debug("mount %s failed: %s", dev, result.stderr.decode(errors="replace").strip())

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
