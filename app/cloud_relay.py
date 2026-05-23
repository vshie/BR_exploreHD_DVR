"""
RTMP cloud relay: spawn one ffmpeg subprocess per MCM RTSP camera that copies
the H.264 elementary stream to a hardcoded RTMP/FLV endpoint, with a watchdog
that respawns dead pipes on a backoff. Audio is dropped (`-an`) — the cameras
don't carry useful audio and the receiving service is video-only.

Equivalent shell command per cam:

    ffmpeg -rtsp_transport udp -i <rtsp_url_from_mcm> \\
           -c:v copy -an -f flv rtmp://35.85.229.226/live/bom_cam0N

Design notes
------------

  - Modeled on app/recorder.py: a per-cam Relay object owns a Popen and a
    watchdog thread; a RelayManager orchestrates start/stop for the whole
    fleet. We deliberately keep this independent of the recorder so a
    misbehaving cloud egress never disturbs disk recording.
  - No stall detector is needed here: ffmpeg's RTMP write blocks until the
    server ACKs, so a dropped uplink shows up as the subprocess exiting
    cleanly with a non-zero status. The watchdog respawns it.
  - The destination URL and the cam-index → stream-key mapping are HARDCODED
    per the project requirement that there is no per-deployment knob for
    the RTMP destination — only an enable/disable toggle. Add a new key
    here if a 5th camera is ever introduced.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Hardcoded cloud destination. The RTMP server URL never varies between
# installs, and the per-cam stream keys map cam index 0..3 onto the
# bom_cam01..bom_cam04 buckets the receiving service is provisioned for.
RTMP_BASE_URL = "rtmp://35.85.229.226/live"
RTMP_STREAM_KEYS: List[str] = ["bom_cam01", "bom_cam02", "bom_cam03", "bom_cam04"]

# Watchdog cadence and respawn backoff.
WATCHDOG_INTERVAL_S = 2.0
BACKOFF_START = 1.0
BACKOFF_MAX = 10.0

# How long after spawn we consider the pipeline "established" enough that
# an exit should reset the backoff. A pipeline that runs for >= this many
# seconds is treated as healthy; anything shorter than that and we keep
# growing the backoff.
HEALTHY_RUN_S = 30.0

# Special-case backoff for the RTMP "Already publishing" reject path. When
# our previous ffmpeg dies without cleanly tearing down its RTMP socket
# (network glitch, abrupt MCM RTSP close, container restart, etc.), the
# upstream RTMP server keeps the stale publisher's stream-key slot held
# for its keepalive timeout — typically 30..90 seconds — and refuses any
# new `publish` command for the same key with `Server error: Already
# publishing`. The 1..10 s backoff is much shorter than that timeout, so
# without this special case we end up thrashing: every retry hits the
# same "Already publishing" wall, ffmpeg dies again, and the operator
# sees the cam stuck in "restarting" indefinitely.
#
# 90 s is long enough to comfortably clear the typical RTMP keepalive
# window without making the operator wait forever after a transient
# network drop. Anything shorter than ~60 s tends to race the server.
STALE_PUBLISHER_BACKOFF_S = 90.0

# Stderr line patterns we treat as "the upstream RTMP server is still
# holding our previous publisher session". Matched case-insensitively
# anywhere on the line so wrapping prefixes (`[rtmp @ 0x...]`, etc.)
# don't break detection.
_STALE_PUBLISHER_PATTERNS: List[str] = [
    "already publishing",
    # Some servers reject with a generic "publish failed" / "publishing
    # in progress" message; treat them as the same race so we wait the
    # server out instead of thrashing.
    "publishing in progress",
    "stream is busy",
]


def _stream_key_for_index(cam_index: int) -> Optional[str]:
    if 0 <= cam_index < len(RTMP_STREAM_KEYS):
        return RTMP_STREAM_KEYS[cam_index]
    return None


def _rtmp_url_for_index(cam_index: int) -> Optional[str]:
    key = _stream_key_for_index(cam_index)
    if key is None:
        return None
    return f"{RTMP_BASE_URL}/{key}"


class Relay:
    """Single-cam RTMP relay (RTSP in, RTMP out, video-copy, no re-encode)."""

    def __init__(self, index: int, stream: Dict[str, Any]):
        self.index = index
        self.stream = stream
        self.stream_key = _stream_key_for_index(index) or ""
        self.rtmp_url = _rtmp_url_for_index(index) or ""

        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None

        self.state = "idle"
        self.last_error = ""
        self.ff_errors = 0
        self.restart_count = 0
        self.started_at: Optional[float] = None
        self._pipeline_start_mono: float = 0.0

        # Set by `_stderr_reader` when ffmpeg's stderr matches one of the
        # `_STALE_PUBLISHER_PATTERNS`. The watchdog reads this flag on
        # subprocess exit to decide whether to apply the much longer
        # `STALE_PUBLISHER_BACKOFF_S` instead of the regular 1..10 s
        # backoff. Always cleared just before each new spawn so a fresh
        # stale-publisher hit produces a fresh long-backoff decision.
        self._stale_publisher_seen: bool = False
        # Cumulative count of long-backoff waits. Surfaced via status_dict
        # so the UI can render "waiting for server to release stream key
        # (Nth time)" rather than a confusing growing restart_count.
        self.stale_publisher_waits: int = 0

    def _build_cmd(self) -> List[str]:
        url = self.stream["rtsp_url"]
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-rtsp_transport", "udp",
            "-i", url,
            "-c:v", "copy",
            "-an",
            "-f", "flv",
            self.rtmp_url,
        ]

    def _stderr_reader(self):
        if not self._proc or not self._proc.stderr:
            return
        try:
            for line in iter(self._proc.stderr.readline, b""):
                if not line:
                    break
                text = line.decode(errors="replace").strip()
                if not text:
                    continue
                low = text.lower()
                # Detect the RTMP server holding our previous publisher
                # session. We set the flag (don't sleep here — the
                # watchdog handles backoff after exit) and tag the error
                # message so the UI shows a useful explanation instead
                # of a bare "Already publishing".
                if any(p in low for p in _STALE_PUBLISHER_PATTERNS):
                    self._stale_publisher_seen = True
                    self.ff_errors += 1
                    self.last_error = (
                        f"RTMP server still holds the previous publisher "
                        f"session for {self.stream_key!r}; waiting "
                        f"{int(STALE_PUBLISHER_BACKOFF_S)}s for it to "
                        f"time out. ffmpeg said: {text[:200]}"
                    )
                    logger.warning(
                        f"[relay{self.index}] stale RTMP publisher: {text}"
                    )
                elif "error" in low or "failed" in low:
                    self.ff_errors += 1
                    self.last_error = text[:300]
                    logger.error(f"[relay{self.index}] ffmpeg: {text}")
                elif "warning" in low:
                    logger.warning(f"[relay{self.index}] ffmpeg: {text}")
                else:
                    logger.debug(f"[relay{self.index}] ffmpeg: {text}")
        except Exception as e:
            logger.debug(f"[relay{self.index}] stderr reader end: {e}")

    def _start_pipeline(self) -> bool:
        self._stop_pipeline()
        if not self.rtmp_url:
            self.last_error = (
                f"no RTMP stream key mapped for cam index {self.index}"
            )
            self.state = "skipped"
            return False
        # Reset the stale-publisher signal before each spawn so a fresh
        # "Already publishing" hit during this attempt produces a fresh
        # long-backoff decision; carrying it across attempts would lock
        # us into the 90 s wait forever once we'd seen it once.
        self._stale_publisher_seen = False
        cmd = self._build_cmd()
        logger.info(f"[relay{self.index}] {' '.join(cmd)}")
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
            )
        except Exception as e:
            logger.exception(f"[relay{self.index}] Popen failed: {e}")
            self._proc = None
            self.last_error = f"Popen failed: {e}"
            return False
        self._stderr_thread = threading.Thread(
            target=self._stderr_reader, daemon=True, name=f"relay-stderr-{self.index}"
        )
        self._stderr_thread.start()
        time.sleep(1.0)
        if self._proc.poll() is not None:
            err = b""
            if self._proc.stderr:
                try:
                    err = self._proc.stderr.read() or b""
                except Exception:
                    err = b""
            msg = err.decode(errors="replace").strip() or "ffmpeg exited immediately"
            logger.error(f"[relay{self.index}] died on start: {msg}")
            self.last_error = msg[:300]
            self._proc = None
            return False
        self.state = "running"
        self._pipeline_start_mono = time.monotonic()
        self.started_at = time.time()
        return True

    def _stop_pipeline(self):
        with self._lock:
            proc = self._proc
            self._proc = None
        if proc and proc.poll() is None:
            try:
                proc.send_signal(signal.SIGINT)
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                    proc.wait(timeout=2)
                except Exception:
                    pass

    def _watch_loop(self):
        backoff = BACKOFF_START
        while not self._stop.is_set():
            with self._lock:
                proc = self._proc
            if proc is None or proc.poll() is not None:
                if self._stop.is_set():
                    break
                ran_for = (
                    time.monotonic() - self._pipeline_start_mono
                    if self._pipeline_start_mono
                    else 0.0
                )
                if ran_for >= HEALTHY_RUN_S:
                    backoff = BACKOFF_START
                # If the just-died ffmpeg saw "Already publishing" on
                # stderr, the RTMP server is still holding our previous
                # publisher slot and any retry shorter than its keepalive
                # timeout (~30..90s) will keep hitting the same wall.
                # Wait it out before respawning. Use the interruptible
                # `_stop.wait` so a UI toggle-off / shutdown doesn't have
                # to sit through the full 90 s sleep.
                if self._stale_publisher_seen:
                    self.stale_publisher_waits += 1
                    self.state = "waiting_for_rtmp_release"
                    logger.warning(
                        f"[relay{self.index}] RTMP server still publishing; "
                        f"sleeping {int(STALE_PUBLISHER_BACKOFF_S)}s before "
                        f"retry (wait #{self.stale_publisher_waits})"
                    )
                    if self._stop.wait(STALE_PUBLISHER_BACKOFF_S):
                        break
                    # Reset the regular backoff so once the server
                    # releases the slot we don't immediately apply the
                    # short-attempt backoff on top of the long wait.
                    backoff = BACKOFF_START
                    self._stale_publisher_seen = False
                self.state = "restarting"
                self.restart_count += 1
                logger.info(
                    f"[relay{self.index}] starting ffmpeg "
                    f"(attempt {self.restart_count})"
                )
                ok = self._start_pipeline()
                if not ok:
                    if self.state != "skipped":
                        self.state = "error"
                    if self.state == "skipped":
                        return
                    if self._stop.wait(backoff):
                        break
                    backoff = min(BACKOFF_MAX, backoff * 1.5)
                    continue
            time.sleep(WATCHDOG_INTERVAL_S)
        self._stop_pipeline()
        self.state = "stopped"

    def start(self):
        self._stop.clear()
        if self._thread and self._thread.is_alive():
            return
        if not self.rtmp_url:
            self.state = "skipped"
            self.last_error = (
                f"no RTMP stream key mapped for cam index {self.index}"
            )
            return
        self._thread = threading.Thread(
            target=self._watch_loop, daemon=True, name=f"relay-{self.index}"
        )
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None

    def status_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "name": self.stream.get("name"),
            "stream_id": self.stream.get("stream_id"),
            "rtsp_url": self.stream.get("rtsp_url"),
            "stream_key": self.stream_key,
            "rtmp_url": self.rtmp_url,
            "state": self.state,
            "last_error": self.last_error,
            "ff_errors": self.ff_errors,
            "restart_count": self.restart_count,
            "stale_publisher_waits": self.stale_publisher_waits,
            "started_at": self.started_at,
        }


class RelayManager:
    """Owns a Relay per stream and orchestrates start/stop."""

    def __init__(self, streams: List[Dict[str, Any]]):
        self.relays: List[Relay] = []
        for i, s in enumerate(streams):
            self.relays.append(Relay(i, s))

    def start_all(self):
        if not self.relays:
            return
        logger.info(f"Starting {len(self.relays)} cloud relay(s) → {RTMP_BASE_URL}")
        for r in self.relays:
            r.start()

    def stop_all(self):
        for r in self.relays:
            r.stop()

    def status(self) -> List[Dict[str, Any]]:
        return [r.status_dict() for r in self.relays]


# ---------------------------------------------------------------------------
# Module-level orchestrator used by main.py / boot_manager.py
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_manager: Optional[RelayManager] = None
_streams_provider: Optional[Callable[[], List[Dict[str, Any]]]] = None


def _read_enabled() -> bool:
    """Read the persisted toggle. Imported lazily so cold-start ordering
    issues (settings_store is loaded after this module on some paths) don't
    matter."""
    try:
        from settings_store import load_settings  # local import on purpose
        return bool(load_settings().get("cloud_relay_enabled", True))
    except Exception:
        return True


def configure(streams_provider: Callable[[], List[Dict[str, Any]]]) -> None:
    """Tell the relay where to fetch the current MCM stream list. Called
    once from `main._boot_worker` so toggle-on later can build a manager
    without re-running the boot sequence."""
    global _streams_provider
    with _lock:
        _streams_provider = streams_provider


def start_if_enabled() -> None:
    """Spin up a manager iff the toggle is on. Idempotent."""
    if not _read_enabled():
        logger.info("Cloud relay disabled in settings; not starting")
        return
    start_now()


def start_now() -> None:
    """Force-start regardless of the persisted toggle (used after the toggle
    is flipped to ON)."""
    global _manager
    with _lock:
        provider = _streams_provider
        if _manager is not None:
            return
    if provider is None:
        logger.warning("Cloud relay start requested before configure()")
        return
    streams = provider() or []
    if not streams:
        logger.warning("Cloud relay: no streams from MCM; nothing to relay")
        return
    mgr = RelayManager(streams)
    with _lock:
        _manager = mgr
    mgr.start_all()


def stop_now() -> None:
    """Tear down the manager. Idempotent."""
    global _manager
    with _lock:
        mgr = _manager
        _manager = None
    if mgr is not None:
        mgr.stop_all()


def apply_settings_change() -> None:
    """React to a /cloud/toggle (or /settings) POST that flipped the toggle."""
    if _read_enabled():
        start_now()
    else:
        stop_now()


def is_running() -> bool:
    with _lock:
        return _manager is not None


def status() -> Dict[str, Any]:
    with _lock:
        mgr = _manager
        running = mgr is not None
    enabled = _read_enabled()
    cams = mgr.status() if mgr is not None else []
    streaming = sum(1 for c in cams if c.get("state") == "running")
    waiting = sum(
        1 for c in cams if c.get("state") == "waiting_for_rtmp_release"
    )
    total_restarts = sum(int(c.get("restart_count", 0) or 0) for c in cams)
    total_stale_waits = sum(
        int(c.get("stale_publisher_waits", 0) or 0) for c in cams
    )
    return {
        "enabled": enabled,
        "running": running,
        "rtmp_base_url": RTMP_BASE_URL,
        "stream_keys": list(RTMP_STREAM_KEYS),
        "cams": cams,
        "streaming_count": streaming,
        "waiting_for_rtmp_release_count": waiting,
        "total_count": len(cams),
        "total_restarts": total_restarts,
        "total_stale_publisher_waits": total_stale_waits,
        "stale_publisher_backoff_s": int(STALE_PUBLISHER_BACKOFF_S),
    }


def summary() -> Dict[str, Any]:
    """Compact block embedded in /status."""
    st = status()
    return {
        "enabled": st["enabled"],
        "running": st["running"],
        "streaming_count": st["streaming_count"],
        "waiting_for_rtmp_release_count": st["waiting_for_rtmp_release_count"],
        "total_count": st["total_count"],
        "total_restarts": st["total_restarts"],
        "total_stale_publisher_waits": st["total_stale_publisher_waits"],
        "stale_publisher_backoff_s": st["stale_publisher_backoff_s"],
        "rtmp_base_url": st["rtmp_base_url"],
    }
