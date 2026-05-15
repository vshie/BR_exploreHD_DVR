"""
NeuralX continuous uploader: ship each closed MPEG-TS segment to the NeuralX
test endpoint, persist per-file upload state across restarts, and optionally
delete the local copy after a successful upload once free space drops below
a configured threshold.

Design overview
---------------

The recorder (`app/recorder.py`) renames each closed `seg_NNNNN.ts` to
`YYYYMMDD_HHMMSS.ts` inside `cam_<index>_<sanitized_name>/` once
splitmuxsink rolls to the next segment. That rename is our "closed segment"
signal — any `.ts` file under the recordings tree whose name matches the
timestamped pattern is safe to upload (the active segment is still under
the `seg_NNNNN.ts` template and is intentionally excluded by the regex).

The uploader runs two thread families:

  - A scanner thread that walks every storage root (SD + USB), enqueues
    closed segments that don't appear in the on-disk state file as `done`,
    and naps for `_SCAN_INTERVAL_S` between sweeps. It also wakes immediately
    when `wake()` is called (e.g. from `/neuralx/retry`).

  - 1..N worker threads that pop entries off the queue, GET a presigned URL
    from the configured endpoint, PUT the file bytes, and update state.

State is persisted to `/app/recordings/.br_explorehd_dvr_neuralx_state.json`
(same bind-mounted directory as `settings_store.SETTINGS_PATH`), atomically
via `tempfile + os.replace`, so the queue and per-file status survive
container restarts.

The endpoint protocol matches the NeuralX integration guide verbatim:

  GET  {endpoint}?camera_id=<01..04>&filename=<upload_name>
       -> JSON { "upload_url": "https://...presigned..." }
  PUT  <upload_url>  (raw file bytes; AWS signature already embedded)
       -> 2xx on success
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

import usb_storage
from settings_store import (
    NEURALX_ALLOWED_CAM_IDS,
    NEURALX_DEFAULT_CAM_MAP,
    NEURALX_TOKEN_RE,
    load_settings,
)

logger = logging.getLogger(__name__)

# Same bind mount BlueOS uses for recordings; survives reboot when host path is bound.
STATE_DIR = "/app/recordings"
STATE_PATH = os.path.join(STATE_DIR, ".br_explorehd_dvr_neuralx_state.json")
STATE_VERSION = 1

RECORDINGS_LOCAL = "/app/recordings"

# Filename patterns shared with main.py. We deliberately match only the
# finalized timestamped name produced by recorder._format_segment_filename —
# never the active `seg_NNNNN.ts` template.
_CLOSED_SEG_RE = re.compile(r"^(\d{8})_(\d{6})(?:_\d+)?\.ts$")
_CAM_DIR_RE = re.compile(r"^cam_(\d+)_(.+)$")
_DATE_RE = re.compile(r"^\d{8}$")

# Allowed upload extensions, per the NeuralX integration guide.
_ALLOWED_EXT = {".mp4", ".ts", ".m4s", ".mov"}

# Scanner sleep between full sweeps. Lower than the typical segment length
# (300 s default) so a freshly-closed segment is picked up within seconds.
_SCAN_INTERVAL_S = 10.0

# Stability cushion: ignore files younger than this. splitmuxsink
# async-finalize=true closes the previous segment off-thread, and the
# recorder's rename happens after that closes — but we still keep a small
# guard so an in-flight rename can't be observed mid-write.
_MIN_AGE_S = 5.0

# Retry backoff schedule (seconds). The final value is the steady-state retry
# interval — we never give up entirely on a failed file because transient
# network outages should heal eventually.
_BACKOFF_SCHEDULE = (30.0, 120.0, 600.0, 1800.0, 3600.0)

# HTTP timeouts (mirror the PDF reference script).
_GET_TIMEOUT_S = 15.0
_PUT_TIMEOUT_S = 600.0

# Recent-uploads ring buffer size for the status payload.
_RECENT_LIMIT = 20


def _now() -> float:
    return time.time()


def _next_retry_delay(attempts: int) -> float:
    if attempts <= 0:
        return _BACKOFF_SCHEDULE[0]
    idx = min(attempts - 1, len(_BACKOFF_SCHEDULE) - 1)
    return _BACKOFF_SCHEDULE[idx]


def _storage_roots() -> List[str]:
    """Local SD root, plus the USB BR_exploreHD_DVR dir when mounted."""
    roots = [RECORDINGS_LOCAL]
    try:
        if usb_storage.is_mounted():
            roots.append(os.path.join(usb_storage.USB_MOUNT_POINT, usb_storage.DVR_DIR))
    except Exception:
        pass
    return roots


def _free_mb_for(path: str) -> Optional[float]:
    """Free megabytes on the volume holding `path` (or its nearest existing parent)."""
    p = path
    while p and not os.path.exists(p):
        np = os.path.dirname(p)
        if np == p:
            break
        p = np
    if not p or not os.path.exists(p):
        return None
    try:
        usage = shutil.disk_usage(p)
        return usage.free / (1024 * 1024)
    except OSError:
        return None


def _sanitize_basename(name: str) -> Optional[str]:
    """Return `name` if it satisfies the NeuralX filename whitelist; else None.

    The PDF allows `.mp4 / .ts / .m4s / .mov` extensions and filename
    characters `[A-Za-z0-9._-]`. Our closed-segment names (YYYYMMDD_HHMMSS.ts)
    are already inside this whitelist by construction, but defensive
    validation keeps a future rename refactor from silently breaking uploads.
    """
    ext = os.path.splitext(name)[1].lower()
    if ext not in _ALLOWED_EXT:
        return None
    if not re.match(r"^[A-Za-z0-9._-]+$", name):
        return None
    return name


def _build_upload_name(node_id: str, basename: str) -> Optional[str]:
    """`<node_id>_<basename>` if both halves are valid, else None.

    Concatenation widens the "Pi pool" without colliding on the shared test
    bucket — each Pi sets its own `node_id` and the resulting upload names
    are unique across nodes even when two cams on different nodes share
    `camera_id`.
    """
    if not node_id or not NEURALX_TOKEN_RE.match(node_id):
        return None
    safe = _sanitize_basename(basename)
    if not safe:
        return None
    return f"{node_id}_{safe}"


# ---------------------------------------------------------------------------
# Persistent state
# ---------------------------------------------------------------------------


class _State:
    """In-memory cache of the upload state file with atomic write-through.

    Schema (see also docstring at top of file):
      {
        "version": 1,
        "totals": {"files": int, "bytes": int, "last_success_epoch": float},
        "files": {
          "<abs_path>": {
            "status": "pending" | "done" | "done_deleted" | "failed",
            "upload_name": str,
            "camera_id": "01".."04",
            "bytes": int,
            "uploaded_epoch": float,
            "duration_s": float,
            "mbps": float,
            "attempts": int,
            "next_retry_epoch": float,   # only when status=="failed"
            "last_error": str,           # only when status=="failed"
          }
        },
        "recent": [ {filename, camera_id, bytes, mbps, status, epoch, error?}, ... ]
      }
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.data: Dict[str, Any] = {
            "version": STATE_VERSION,
            "totals": {"files": 0, "bytes": 0, "last_success_epoch": 0.0},
            "files": {},
            "recent": [],
        }
        self._load()

    def _load(self) -> None:
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                return
            if int(raw.get("version", 0)) != STATE_VERSION:
                logger.warning(
                    "Ignoring NeuralX state with version=%s (expected %s)",
                    raw.get("version"), STATE_VERSION,
                )
                return
            totals = raw.get("totals") or {}
            files = raw.get("files") or {}
            recent = raw.get("recent") or []
            if isinstance(totals, dict):
                self.data["totals"] = {
                    "files": int(totals.get("files", 0) or 0),
                    "bytes": int(totals.get("bytes", 0) or 0),
                    "last_success_epoch": float(totals.get("last_success_epoch", 0.0) or 0.0),
                }
            if isinstance(files, dict):
                self.data["files"] = {k: dict(v) for k, v in files.items() if isinstance(v, dict)}
            if isinstance(recent, list):
                self.data["recent"] = [dict(r) for r in recent if isinstance(r, dict)][-_RECENT_LIMIT:]
        except FileNotFoundError:
            return
        except Exception as e:
            logger.warning("Could not read NeuralX state %s: %s", STATE_PATH, e)

    def _persist_locked(self) -> None:
        os.makedirs(STATE_DIR, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".neuralx_state.", dir=STATE_DIR)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, sort_keys=True)
                f.write("\n")
            os.replace(tmp, STATE_PATH)
        except Exception as e:
            logger.exception("Failed to persist NeuralX state: %s", e)
            try:
                os.remove(tmp)
            except OSError:
                pass

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "totals": dict(self.data["totals"]),
                "files": {k: dict(v) for k, v in self.data["files"].items()},
                "recent": list(self.data["recent"]),
            }

    def get_file(self, path: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            v = self.data["files"].get(path)
            return dict(v) if isinstance(v, dict) else None

    def is_done(self, path: str) -> bool:
        v = self.get_file(path)
        return bool(v) and v.get("status") in ("done", "done_deleted")

    def set_pending(self, path: str) -> None:
        with self._lock:
            cur = self.data["files"].get(path) or {}
            cur["status"] = "pending"
            cur.pop("next_retry_epoch", None)
            cur.pop("last_error", None)
            self.data["files"][path] = cur
            self._persist_locked()

    def mark_success(
        self,
        path: str,
        *,
        upload_name: str,
        camera_id: str,
        size_bytes: int,
        duration_s: float,
        mbps: float,
        deleted: bool,
    ) -> None:
        with self._lock:
            cur = self.data["files"].get(path) or {}
            attempts = int(cur.get("attempts", 0) or 0) + 1
            entry = {
                "status": "done_deleted" if deleted else "done",
                "upload_name": upload_name,
                "camera_id": camera_id,
                "bytes": int(size_bytes),
                "uploaded_epoch": _now(),
                "duration_s": round(float(duration_s), 3),
                "mbps": round(float(mbps), 2),
                "attempts": attempts,
            }
            self.data["files"][path] = entry
            t = self.data["totals"]
            t["files"] = int(t.get("files", 0) or 0) + 1
            t["bytes"] = int(t.get("bytes", 0) or 0) + int(size_bytes)
            t["last_success_epoch"] = entry["uploaded_epoch"]
            self._push_recent_locked(
                {
                    "filename": upload_name,
                    "camera_id": camera_id,
                    "bytes": int(size_bytes),
                    "mbps": entry["mbps"],
                    "status": entry["status"],
                    "epoch": entry["uploaded_epoch"],
                }
            )
            self._persist_locked()

    def mark_failure(self, path: str, error: str, *, camera_id: Optional[str] = None) -> None:
        with self._lock:
            cur = self.data["files"].get(path) or {}
            attempts = int(cur.get("attempts", 0) or 0) + 1
            delay = _next_retry_delay(attempts)
            cur.update(
                status="failed",
                attempts=attempts,
                last_error=error[:500],
                next_retry_epoch=_now() + delay,
            )
            if camera_id:
                cur["camera_id"] = camera_id
            self.data["files"][path] = cur
            self._push_recent_locked(
                {
                    "filename": os.path.basename(path),
                    "camera_id": cur.get("camera_id", ""),
                    "bytes": int(cur.get("bytes", 0) or 0),
                    "mbps": 0.0,
                    "status": "failed",
                    "epoch": _now(),
                    "error": error[:200],
                }
            )
            self._persist_locked()

    def _push_recent_locked(self, entry: Dict[str, Any]) -> None:
        recent = self.data["recent"]
        recent.append(entry)
        if len(recent) > _RECENT_LIMIT:
            del recent[: len(recent) - _RECENT_LIMIT]

    def reset_failed_for_retry(self) -> int:
        """Force every failed entry to be eligible immediately. Returns the count."""
        with self._lock:
            n = 0
            now = _now()
            for v in self.data["files"].values():
                if v.get("status") == "failed":
                    v["next_retry_epoch"] = now
                    n += 1
            if n:
                self._persist_locked()
            return n

    def queue_counts(self) -> Tuple[int, int]:
        """(pending, failed) — failed means scheduled for retry."""
        with self._lock:
            pending = 0
            failed = 0
            for v in self.data["files"].values():
                s = v.get("status")
                if s == "pending":
                    pending += 1
                elif s == "failed":
                    failed += 1
            return pending, failed


# ---------------------------------------------------------------------------
# Scanner / worker
# ---------------------------------------------------------------------------


# Sentinel that wakes a worker so it can re-check the stop event when the
# manager shuts down.
_STOP_SENTINEL: Tuple[str, str] = ("", "")


def _walk_closed_segments(roots: List[str]) -> List[Tuple[str, int]]:
    """Return [(absolute_path, cam_index), ...] for every closed segment under `roots`.

    Layout (see README): `<root>/<YYYYMMDD>/<session_id>/cam_<n>_<name>/<ts_file>.ts`.
    """
    out: List[Tuple[str, int]] = []
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        try:
            day_entries = os.listdir(root)
        except OSError:
            continue
        for day in day_entries:
            if not _DATE_RE.match(day):
                continue
            day_path = os.path.join(root, day)
            if not os.path.isdir(day_path):
                continue
            try:
                sessions = os.listdir(day_path)
            except OSError:
                continue
            for sess in sessions:
                sp = os.path.join(day_path, sess)
                if not os.path.isdir(sp):
                    continue
                try:
                    cam_dirs = os.listdir(sp)
                except OSError:
                    continue
                for cam_dir in cam_dirs:
                    m = _CAM_DIR_RE.match(cam_dir)
                    if not m:
                        continue
                    try:
                        cam_index = int(m.group(1))
                    except ValueError:
                        continue
                    cam_path = os.path.join(sp, cam_dir)
                    if not os.path.isdir(cam_path):
                        continue
                    try:
                        files = os.listdir(cam_path)
                    except OSError:
                        continue
                    for fn in files:
                        if not _CLOSED_SEG_RE.match(fn):
                            continue
                        out.append((os.path.join(cam_path, fn), cam_index))
    return out


class NeuralXUploader:
    """Long-running uploader. Safe to start/stop/reload at runtime.

    The Flask layer holds a single module-level instance (see
    `get_or_create()` / `apply_settings()`) and reaches into it from request
    handlers — no per-request threads.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = _State()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._scanner: Optional[threading.Thread] = None
        self._workers: List[threading.Thread] = []
        self._queue: List[Tuple[str, int]] = []  # (path, cam_index)
        self._queue_cv = threading.Condition()
        self._in_flight: int = 0
        self._last_scan_at: float = 0.0
        self._last_error: str = ""

        # Hot config (refreshed from settings on every scan).
        self._enabled: bool = False
        self._node_id: str = ""
        self._endpoint: str = ""
        self._cam_map: Dict[str, str] = dict(NEURALX_DEFAULT_CAM_MAP)
        self._max_concurrent: int = 1
        self._delete_below_free_mb: int = 0

    # -- lifecycle -----------------------------------------------------------

    def _read_settings(self) -> None:
        try:
            s = load_settings()
        except Exception as e:
            logger.warning("NeuralX: could not read settings (%s); leaving disabled", e)
            self._enabled = False
            return
        self._enabled = bool(s.get("neuralx_enabled", False))
        self._node_id = str(s.get("neuralx_node_id", "") or "")
        self._endpoint = str(s.get("neuralx_endpoint", "") or "")
        cm = s.get("neuralx_cam_map") or {}
        if isinstance(cm, dict):
            self._cam_map = {str(k): str(v) for k, v in cm.items()}
        else:
            self._cam_map = dict(NEURALX_DEFAULT_CAM_MAP)
        try:
            self._max_concurrent = int(s.get("neuralx_max_concurrent", 1) or 1)
        except (TypeError, ValueError):
            self._max_concurrent = 1
        try:
            self._delete_below_free_mb = int(s.get("neuralx_delete_below_free_mb", 0) or 0)
        except (TypeError, ValueError):
            self._delete_below_free_mb = 0

    def start(self) -> None:
        """Start the scanner and worker threads if not already running."""
        with self._lock:
            if self._scanner and self._scanner.is_alive():
                return
            self._read_settings()
            if not self._enabled:
                logger.info("NeuralX uploader start requested but disabled in settings")
                return
            self._stop.clear()
            self._scanner = threading.Thread(
                target=self._scan_loop, name="neuralx-scan", daemon=True
            )
            self._scanner.start()
            self._workers = []
            for i in range(max(1, self._max_concurrent)):
                t = threading.Thread(
                    target=self._worker_loop,
                    name=f"neuralx-up-{i}",
                    daemon=True,
                )
                t.start()
                self._workers.append(t)
            logger.info(
                "NeuralX uploader started (node_id=%s, workers=%d, delete_below_free_mb=%d)",
                self._node_id, len(self._workers), self._delete_below_free_mb,
            )

    def stop(self, *, join_timeout: float = 5.0) -> None:
        with self._lock:
            if not (self._scanner and self._scanner.is_alive()) and not self._workers:
                return
            self._stop.set()
            # Wake the scanner.
            self._wake.set()
            # Wake every worker by enqueueing one sentinel per worker.
            with self._queue_cv:
                for _ in range(len(self._workers) or 1):
                    self._queue.append(_STOP_SENTINEL)
                self._queue_cv.notify_all()
        scanner = self._scanner
        workers = list(self._workers)
        if scanner:
            scanner.join(timeout=join_timeout)
        for t in workers:
            t.join(timeout=join_timeout)
        with self._lock:
            self._scanner = None
            self._workers = []
            self._queue.clear()
            self._in_flight = 0
        logger.info("NeuralX uploader stopped")

    def apply_settings(self) -> None:
        """React to a /neuralx/settings POST: start, stop, or restart workers."""
        prev_enabled = self._enabled
        prev_workers = self._max_concurrent
        self._read_settings()
        if self._enabled and not prev_enabled:
            self.start()
            return
        if not self._enabled and prev_enabled:
            self.stop()
            return
        if self._enabled and self._max_concurrent != prev_workers:
            # Restart workers so the new pool size takes effect.
            self.stop()
            self.start()
            return
        # Same enabled state & worker count → just kick a scan with the new
        # config (endpoint / cam_map / node_id can all change without a
        # thread restart).
        self.wake()

    def wake(self) -> None:
        self._wake.set()

    # -- scanner -------------------------------------------------------------

    def _scan_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._read_settings()
                if not self._enabled:
                    # Disabled mid-flight: drain and exit. The caller
                    # (`apply_settings`) is responsible for stopping workers.
                    break
                self._scan_once()
            except Exception as e:
                logger.exception("NeuralX scanner error: %s", e)
                self._last_error = f"scanner: {e}"
            self._last_scan_at = _now()
            self._wake.wait(_SCAN_INTERVAL_S)
            self._wake.clear()

    def _scan_once(self) -> None:
        candidates = _walk_closed_segments(_storage_roots())
        if not candidates:
            return
        now = _now()
        with self._queue_cv:
            queued_paths = {p for p, _ in self._queue if p}
            for path, cam_index in candidates:
                if path in queued_paths:
                    continue
                if self._state.is_done(path):
                    continue
                # Stability guard: skip very recently modified files.
                try:
                    mtime = os.path.getmtime(path)
                except OSError:
                    continue
                if (now - mtime) < _MIN_AGE_S:
                    continue
                # Failed entries respect the backoff schedule.
                entry = self._state.get_file(path)
                if entry and entry.get("status") == "failed":
                    next_at = float(entry.get("next_retry_epoch", 0.0) or 0.0)
                    if now < next_at:
                        continue
                self._state.set_pending(path)
                self._queue.append((path, cam_index))
                queued_paths.add(path)
            self._queue_cv.notify_all()

    # -- workers -------------------------------------------------------------

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            with self._queue_cv:
                while not self._queue and not self._stop.is_set():
                    self._queue_cv.wait(timeout=1.0)
                if self._stop.is_set():
                    return
                path, cam_index = self._queue.pop(0)
            if path == "" and cam_index == 0:
                # _STOP_SENTINEL
                return
            with self._lock:
                self._in_flight += 1
            try:
                self._upload_one(path, cam_index)
            except Exception as e:
                logger.exception("NeuralX worker: unexpected error on %s: %s", path, e)
                self._state.mark_failure(path, f"worker exception: {e}")
            finally:
                with self._lock:
                    self._in_flight = max(0, self._in_flight - 1)

    def _upload_one(self, path: str, cam_index: int) -> None:
        if not os.path.isfile(path):
            self._state.mark_failure(path, "file missing at upload time")
            return
        cam_id = self._cam_map.get(str(cam_index))
        if cam_id not in NEURALX_ALLOWED_CAM_IDS:
            self._state.mark_failure(path, f"no camera_id mapped for cam index {cam_index}")
            return
        basename = os.path.basename(path)
        upload_name = _build_upload_name(self._node_id, basename)
        if not upload_name:
            self._state.mark_failure(
                path,
                f"node_id or filename rejected by NeuralX whitelist "
                f"(node_id={self._node_id!r}, basename={basename!r})",
                camera_id=cam_id,
            )
            return
        try:
            size_bytes = os.path.getsize(path)
        except OSError as e:
            self._state.mark_failure(path, f"stat failed: {e}", camera_id=cam_id)
            return

        endpoint = self._endpoint
        if not endpoint:
            self._state.mark_failure(path, "neuralx_endpoint is empty", camera_id=cam_id)
            return

        logger.info(
            "NeuralX uploading %s (%.1f MB) as %s [camera_id=%s]",
            basename, size_bytes / (1024 * 1024), upload_name, cam_id,
        )
        try:
            # Step 1 — presign request.
            r = requests.get(
                endpoint,
                params={"camera_id": cam_id, "filename": upload_name},
                timeout=_GET_TIMEOUT_S,
            )
            r.raise_for_status()
            data = r.json()
            upload_url = data.get("upload_url") if isinstance(data, dict) else None
            if not upload_url:
                raise ValueError(f"presign response missing upload_url: {data!r}")

            # Step 2 — PUT bytes. Streamed from disk; `requests` will use
            # chunked Transfer-Encoding when handed a file-like object, which
            # matches what the PDF reference script does.
            t0 = time.monotonic()
            with open(path, "rb") as f:
                r2 = requests.put(upload_url, data=f, timeout=_PUT_TIMEOUT_S)
            r2.raise_for_status()
            duration = max(1e-3, time.monotonic() - t0)
            mbps = (size_bytes * 8) / duration / 1_000_000
        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            status = resp.status_code if resp is not None else "?"
            body = ""
            if resp is not None:
                try:
                    body = resp.text[:200]
                except Exception:
                    body = ""
            msg = f"HTTP {status}: {body or e}"
            self._last_error = msg
            self._state.mark_failure(path, msg, camera_id=cam_id)
            logger.warning("NeuralX upload failed for %s: %s", basename, msg)
            return
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            self._last_error = msg
            self._state.mark_failure(path, msg, camera_id=cam_id)
            logger.warning("NeuralX upload failed for %s: %s", basename, msg)
            return

        # Success — optionally delete to reclaim space.
        deleted = False
        threshold = max(0, int(self._delete_below_free_mb))
        if threshold > 0:
            free_mb = _free_mb_for(path)
            if free_mb is not None and free_mb < threshold:
                try:
                    os.remove(path)
                    deleted = True
                    logger.info(
                        "NeuralX deleted local %s (free %.0f MB < %d MB threshold)",
                        path, free_mb, threshold,
                    )
                except OSError as e:
                    logger.warning("NeuralX could not delete %s after upload: %s", path, e)
        self._state.mark_success(
            path,
            upload_name=upload_name,
            camera_id=cam_id,
            size_bytes=size_bytes,
            duration_s=duration,
            mbps=mbps,
            deleted=deleted,
        )
        logger.info(
            "NeuralX uploaded %s in %.1fs (%.1f Mbps)%s",
            basename, duration, mbps, " [deleted local]" if deleted else "",
        )

    # -- introspection -------------------------------------------------------

    def retry_failed_now(self) -> int:
        n = self._state.reset_failed_for_retry()
        self.wake()
        return n

    def status(self) -> Dict[str, Any]:
        snap = self._state.snapshot()
        pending = 0
        failed = 0
        for v in snap["files"].values():
            s = v.get("status")
            if s == "pending":
                pending += 1
            elif s == "failed":
                failed += 1
        with self._lock:
            in_flight = self._in_flight
            running = bool(self._scanner and self._scanner.is_alive())
            last_scan = self._last_scan_at
            cam_map = dict(self._cam_map)
        totals = snap["totals"]
        n_files = int(totals.get("files", 0) or 0)
        total_bytes = int(totals.get("bytes", 0) or 0)
        # Mean Mbps across recent successes — cheap and useful for the UI.
        mbps_vals = [
            float(r.get("mbps", 0.0) or 0.0)
            for r in snap["recent"]
            if r.get("status") in ("done", "done_deleted")
        ]
        avg_mbps = round(sum(mbps_vals) / len(mbps_vals), 2) if mbps_vals else 0.0
        return {
            "enabled": self._enabled,
            "running": running,
            "node_id": self._node_id,
            "endpoint": self._endpoint,
            "cam_map": cam_map,
            "max_concurrent": self._max_concurrent,
            "delete_below_free_mb": self._delete_below_free_mb,
            "queue": {
                "pending": pending,
                "failed": failed,
                "in_flight": in_flight,
            },
            "totals": {
                "files": n_files,
                "bytes": total_bytes,
                "last_success_epoch": float(totals.get("last_success_epoch", 0.0) or 0.0),
                "avg_mbps": avg_mbps,
            },
            "recent": list(snap["recent"])[-_RECENT_LIMIT:],
            "last_scan_epoch": last_scan,
            "last_error": self._last_error,
        }

    def summary(self) -> Dict[str, Any]:
        """Compact block embedded in /status (avoid the recent table)."""
        st = self.status()
        return {
            "enabled": st["enabled"],
            "running": st["running"],
            "node_id": st["node_id"],
            "queue": st["queue"],
            "totals": st["totals"],
            "last_error": st["last_error"],
        }


# ---------------------------------------------------------------------------
# Module-level singleton accessor used by main.py
# ---------------------------------------------------------------------------


_singleton: Optional[NeuralXUploader] = None
_singleton_lock = threading.Lock()


def get_or_create() -> NeuralXUploader:
    global _singleton
    with _singleton_lock:
        if _singleton is None:
            _singleton = NeuralXUploader()
        return _singleton


def start_if_enabled() -> None:
    """Called from boot. Starts the uploader iff settings say so."""
    up = get_or_create()
    s = load_settings()
    if bool(s.get("neuralx_enabled", False)):
        up.start()


def apply_settings_change() -> None:
    """Called from POST /neuralx/settings after persisting."""
    get_or_create().apply_settings()


def stop() -> None:
    if _singleton is not None:
        _singleton.stop()
