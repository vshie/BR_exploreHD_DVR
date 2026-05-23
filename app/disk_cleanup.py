"""
External-SSD oldest-session-first cleanup.

Target hardware
---------------

A 256 GB NVMe mounted at `/mnt/usb` (see app/usb_storage.py). The recorder
fills `/mnt/usb/BR_exploreHD_DVR/<YYYYMMDD>/<session_id>/cam_<n>_*/...ts`
in ~5-min segments at ~3 MB/s aggregate across four 1080p H.264 cameras.

Why session-granularity deletion
--------------------------------

Earlier revisions of this extension shipped each closed segment to a cloud
endpoint (NeuralX) and used the per-file upload state as a deletion gate.
That was removed (see commit history) so the device no longer has any
cloud-side acknowledgement to wait on. The next-best invariant is the
session: a session directory is the unit operators reason about in the
Recordings tab, each completed session has a sibling `<session_id>.zip`
built at boot by `boot_manager.zip_unfinished_sessions`, and dropping a
whole session is more useful than dropping isolated segments out of the
middle of one (which would leave gaps in playback).

Policy
------

  - Trigger: free space on the volume that holds `/mnt/usb` drops below
    `CLEANUP_FREE_MB_THRESHOLD` (10 GB on the 256 GB NVMe — i.e. delete
    once the disk is ~96% full). Sweep cadence is `_SWEEP_INTERVAL_S`.
    At ~3 MB/s the recorder writes ~90 MB per tick, and 10 GB headroom
    is ~110× one tick's worth of writes; tighter would risk losing the
    race between sweeps. Looser would waste more of the disk than
    necessary on a small-volume install.
  - Scope: `/mnt/usb/BR_exploreHD_DVR/...` only. Internal SD
    (`/app/recordings`) is NEVER touched here — the recorder's existing
    `MIN_FREE_DISK_MB = 1024` gate handles that volume separately.
  - Target: oldest completed session (and its sibling `<id>.zip`) at a
    time, sorted by the oldest mtime of any file inside the session
    directory. The currently active session (matched by absolute path
    against the value `main` exposes via the `active_session_provider`
    callback) is NEVER deleted.
  - Loop: after each delete, re-stat free space; stop as soon as we're
    above the threshold. If only the active session remains and we're
    still under threshold, log a warning and exit — the recorder's
    1 GB hard-stop in `recorder.MIN_FREE_DISK_MB` is the final safety
    net.

State surfaced for the UI
-------------------------

`status()` returns enough for the Cloud tab to render "X GB free of
256 GB", the threshold, and the last sweep result so operators can tell
at a glance that cleanup is keeping up with recording.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import usb_storage

logger = logging.getLogger(__name__)

# Hardcoded cleanup floor. See module docstring for rationale.
CLEANUP_FREE_MB_THRESHOLD = 10 * 1024  # 10 GB

# Sweep cadence. Long enough not to thrash, short enough that a sequence
# of small sessions can be cleared faster than the recorder can refill.
_SWEEP_INTERVAL_S = 30.0

# Filename / directory shape mirrored from the recorder + boot manager.
_DATE_RE = re.compile(r"^\d{8}$")


def _ssd_root() -> str:
    """Where the recorder writes session directories on the external SSD."""
    return os.path.join(usb_storage.USB_MOUNT_POINT, usb_storage.DVR_DIR)


def _free_mb() -> Optional[float]:
    return usb_storage.get_free_mb()


def _total_mb() -> Optional[float]:
    """Total bytes on the SSD volume, in MB. Used so the UI can render
    'X GB free of Y GB' without the operator doing math."""
    if not usb_storage.is_mounted():
        return None
    try:
        st = os.statvfs(usb_storage.USB_MOUNT_POINT)
        return round((st.f_blocks * st.f_frsize) / (1024 * 1024), 1)
    except Exception:
        return None


def _oldest_mtime(path: str) -> float:
    """Smallest mtime under `path` (recursive). Used as the session's age
    so a session that started yesterday but is still being topped up
    today still ranks oldest by its first segment, not its last."""
    oldest = float("inf")
    try:
        for dirpath, _, files in os.walk(path):
            for fn in files:
                fp = os.path.join(dirpath, fn)
                try:
                    m = os.path.getmtime(fp)
                except OSError:
                    continue
                if m < oldest:
                    oldest = m
    except OSError:
        return float("inf")
    if oldest == float("inf"):
        try:
            return os.path.getmtime(path)
        except OSError:
            return float("inf")
    return oldest


def _list_sessions() -> List[Tuple[str, str, str, float]]:
    """Return `(date, session_id, full_path, oldest_mtime)` for every
    session directory under the SSD root, sorted ascending by mtime."""
    out: List[Tuple[str, str, str, float]] = []
    root = _ssd_root()
    if not os.path.isdir(root):
        return out
    try:
        days = os.listdir(root)
    except OSError:
        return out
    for d in days:
        if not _DATE_RE.match(d):
            continue
        day_path = os.path.join(root, d)
        if not os.path.isdir(day_path):
            continue
        try:
            sessions = os.listdir(day_path)
        except OSError:
            continue
        for s in sessions:
            sp = os.path.join(day_path, s)
            if not os.path.isdir(sp):
                continue
            mt = _oldest_mtime(sp)
            out.append((d, s, sp, mt))
    out.sort(key=lambda t: t[3])
    return out


class _Sweeper:
    """Owns the sweep thread and the most-recent-sweep state for the UI."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._active_session_provider: Optional[Callable[[], Optional[str]]] = None

        self.last_sweep_epoch: float = 0.0
        self.last_deleted_session: Optional[str] = None
        self.last_freed_mb: float = 0.0
        self.last_error: str = ""
        self.deleted_total: int = 0

    def configure(
        self,
        active_session_provider: Callable[[], Optional[str]],
    ) -> None:
        with self._lock:
            self._active_session_provider = active_session_provider

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="disk-cleanup", daemon=True
            )
            self._thread.start()
        logger.info(
            "Disk cleanup sweeper started (threshold=%d MB, interval=%.0fs)",
            CLEANUP_FREE_MB_THRESHOLD, _SWEEP_INTERVAL_S,
        )

    def stop(self, *, join_timeout: float = 3.0) -> None:
        with self._lock:
            t = self._thread
            self._thread = None
        self._stop.set()
        if t:
            t.join(timeout=join_timeout)
        logger.info("Disk cleanup sweeper stopped")

    def _active_session_abspath(self) -> Optional[str]:
        with self._lock:
            provider = self._active_session_provider
        if provider is None:
            return None
        try:
            v = provider()
        except Exception:
            return None
        if not v:
            return None
        try:
            return os.path.abspath(v)
        except Exception:
            return None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._sweep_once()
            except Exception as e:
                logger.exception("Disk cleanup sweep error: %s", e)
                self.last_error = f"sweep: {e}"
            self._stop.wait(_SWEEP_INTERVAL_S)

    def _sweep_once(self) -> None:
        if not usb_storage.is_mounted():
            return
        free = _free_mb()
        if free is None or free >= CLEANUP_FREE_MB_THRESHOLD:
            self.last_sweep_epoch = time.time()
            return

        active_path = self._active_session_abspath()
        sessions = _list_sessions()
        deletions_this_sweep = 0
        last_deleted: Optional[str] = None
        freed_mb = 0.0

        for date, sess_id, sp, _mt in sessions:
            try:
                free_now = _free_mb()
            except Exception:
                free_now = None
            if free_now is not None and free_now >= CLEANUP_FREE_MB_THRESHOLD:
                break
            try:
                if active_path and os.path.abspath(sp) == active_path:
                    continue
            except Exception:
                continue

            try:
                pre_size_bytes = _dir_size_bytes(sp)
            except Exception:
                pre_size_bytes = 0

            ok = self._delete_session(sp, sess_id)
            if not ok:
                continue
            deletions_this_sweep += 1
            last_deleted = f"{date}/{sess_id}"
            freed_mb += pre_size_bytes / (1024 * 1024)

        if deletions_this_sweep == 0:
            free_now = _free_mb()
            if free_now is not None and free_now < CLEANUP_FREE_MB_THRESHOLD:
                logger.warning(
                    "Disk cleanup: free=%.0f MB < threshold=%d MB but no "
                    "deletable session (only the active session remains?). "
                    "Recorder's 1 GB hard-stop is the next line of defence.",
                    free_now, CLEANUP_FREE_MB_THRESHOLD,
                )

        with self._lock:
            self.last_sweep_epoch = time.time()
            self.deleted_total += deletions_this_sweep
            if last_deleted is not None:
                self.last_deleted_session = last_deleted
                self.last_freed_mb = round(freed_mb, 1)

    def _delete_session(self, path: str, session_id: str) -> bool:
        """Remove a session directory and any sibling `<session_id>.zip`."""
        zip_sibling = os.path.join(os.path.dirname(path), f"{session_id}.zip")
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            return False
        except OSError as e:
            logger.warning("Disk cleanup: rmtree %s failed: %s", path, e)
            self.last_error = f"rmtree {path}: {e}"
            return False
        try:
            os.remove(zip_sibling)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning("Disk cleanup: remove %s failed: %s", zip_sibling, e)
        # Best-effort: prune the date directory if it is now empty.
        try:
            day_dir = os.path.dirname(path)
            if os.path.isdir(day_dir) and not os.listdir(day_dir):
                os.rmdir(day_dir)
        except OSError:
            pass
        logger.info("Disk cleanup: deleted session %s", path)
        return True

    def status(self) -> Dict[str, Any]:
        mounted = usb_storage.is_mounted()
        free = _free_mb() if mounted else None
        total = _total_mb() if mounted else None
        with self._lock:
            return {
                "enabled": True,
                "ssd_mounted": mounted,
                "free_mb": free,
                "total_mb": total,
                "threshold_mb": CLEANUP_FREE_MB_THRESHOLD,
                "interval_s": _SWEEP_INTERVAL_S,
                "last_sweep_epoch": self.last_sweep_epoch,
                "last_deleted_session": self.last_deleted_session,
                "last_freed_mb": self.last_freed_mb,
                "deleted_total": self.deleted_total,
                "last_error": self.last_error,
            }


def _dir_size_bytes(path: str) -> int:
    """Recursive sum of file sizes under `path`. Best-effort; missing files
    are silently skipped so a sweep racing with a rename doesn't blow up."""
    total = 0
    try:
        for dirpath, _, files in os.walk(path):
            for fn in files:
                fp = os.path.join(dirpath, fn)
                try:
                    total += os.path.getsize(fp)
                except OSError:
                    pass
    except OSError:
        return 0
    return total


_singleton = _Sweeper()


def configure(active_session_provider: Callable[[], Optional[str]]) -> None:
    _singleton.configure(active_session_provider)


def start() -> None:
    _singleton.start()


def stop() -> None:
    _singleton.stop()


def status() -> Dict[str, Any]:
    return _singleton.status()
