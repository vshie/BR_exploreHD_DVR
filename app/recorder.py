"""
Per-stream GStreamer RTSP -> MPEG-TS segment recorder with watchdog and restart.

Stall isolation (v1.0.15):
  - A 60s RAM-backed queue sits between rtspsrc and splitmuxsink so SD fsync
    pauses up to ~60s are absorbed without backpressuring rtspsrc.
  - The watchdog tracks RTSP ingest (/proc/<pid>/io rchar) rather than file
    growth, so an SD-side pause (file not growing, but queue still draining
    into RAM) no longer looks like a pipeline stall.
  - Segment rotations request a keyframe at roll time so the new file opens
    at a clean I-frame boundary without waiting for the next natural GOP.
  - When recording to internal SD the per-camera start is offset in larger
    steps so the four splitmuxsink rotation fsyncs don't land in the same
    wall-clock second. External media doesn't need that spread.
"""

import logging
import os
import re
import signal
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

MIN_FREE_DISK_MB = 1024
WATCHDOG_INTERVAL_S = 5.0

# Stall detection is now against RTSP ingest (rchar), not file growth. This is
# decoupled from SD write latency so even a 30-45s SD GC pause does not trip
# the watchdog as long as the camera is still sending RTP.
STALL_THRESHOLD_S = 30.0
# RTSP connect + first packet can take 10-20s on a cold start; avoid false restarts.
STALL_GRACE_AFTER_START_S = 30.0
BACKOFF_START = 1.0
BACKOFF_MAX = 10.0

# RAM queue between rtspsrc and splitmuxsink. At ~10 Mbps per camera this
# holds up to ~75 MB per camera worth of buffered video (bounded by the hard
# byte cap below), which is enough to ride through multi-second SD GC pauses
# without stalling the recording pipeline.
QUEUE_MAX_TIME_NS = 60 * 1_000_000_000  # 60 seconds of video in RAM
QUEUE_MAX_BYTES = 100 * 1024 * 1024      # 100 MB hard cap per queue (4 cams -> ~400 MB worst case)

# Per-cam start stagger. Desynchronising the segment rotation boundaries is the
# only software knob we have to stop the four splitmuxsink fsyncs from landing
# in the same wall-clock second on internal SD. External media (NVMe/SSD)
# doesn't need the spread because its worst-case write latency is ~1ms.
START_STAGGER_EXTERNAL_S = 0.5
START_STAGGER_INTERNAL_SD_S = 15.0


def _sanitize_dir_name(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_.-]+", "_", name.strip()) or "cam"
    return s[:80]


def _read_rchar(pid: int) -> Optional[int]:
    """Bytes pulled via read()-family syscalls by the process (includes TCP reads).

    Used as a cheap liveness probe for the RTSP ingest side of the pipeline,
    independent of any downstream file-write activity.
    """
    try:
        with open(f"/proc/{pid}/io", "r") as f:
            for line in f:
                if line.startswith("rchar:"):
                    return int(line.split()[1])
    except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
        return None
    return None


class Recorder:
    def __init__(
        self,
        index: int,
        stream: Dict[str, Any],
        cam_dir: str,
        segment_ns: int,
        disk_free_fn: Callable[[], Optional[float]],
        on_disk_critical: Optional[Callable[[], None]] = None,
    ):
        self.index = index
        self.stream = stream
        self.cam_dir = cam_dir
        os.makedirs(self.cam_dir, exist_ok=True)
        self.segment_ns = segment_ns
        self.disk_free_fn = disk_free_fn
        self.on_disk_critical = on_disk_critical

        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None

        self.state = "idle"
        self.last_error = ""
        self.gst_errors = 0
        self.restart_count = 0
        self.last_grow_time: Optional[float] = None
        self.last_size = 0
        self.current_segment: Optional[str] = None
        self._pipeline_start_mono: float = 0.0

        # RTSP ingest tracking for stall detection (decoupled from disk I/O).
        self._last_rchar: Optional[int] = None
        self._last_rchar_growth_mono: float = 0.0

    def _segment_pattern(self) -> str:
        base = os.path.join(self.cam_dir, "seg_%05d.ts")
        return base

    def _build_cmd(self) -> List[str]:
        url = self.stream["rtsp_url"]
        loc = self._segment_pattern()
        # Pipeline:
        #   rtspsrc -> rtph264depay -> h264parse -> queue(60s/100MB RAM) -> splitmuxsink
        # The large RAM queue absorbs SD write-latency bursts; async-finalize
        # moves old-segment close/fsync off the muxer thread; send-keyframe-requests
        # asks the upstream for a keyframe at roll time so new segments open cleanly.
        return [
            "gst-launch-1.0",
            "-e",
            "rtspsrc",
            f"location={url}",
            "protocols=tcp",
            "latency=200",
            "!",
            "rtph264depay",
            "!",
            "h264parse",
            "config-interval=1",
            "!",
            "queue",
            "max-size-buffers=0",
            f"max-size-bytes={QUEUE_MAX_BYTES}",
            f"max-size-time={QUEUE_MAX_TIME_NS}",
            "leaky=no",
            "!",
            "splitmuxsink",
            "muxer-factory=mpegtsmux",
            "async-finalize=true",
            "send-keyframe-requests=true",
            f"max-size-time={self.segment_ns}",
            f"location={loc}",
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
                if "error" in low or "assertion" in low:
                    self.gst_errors += 1
                    logger.error(f"[cam{self.index}] gst: {text}")
                elif "warning" in low:
                    logger.warning(f"[cam{self.index}] gst: {text}")
                else:
                    logger.debug(f"[cam{self.index}] gst: {text}")
        except Exception as e:
            logger.debug(f"[cam{self.index}] stderr reader end: {e}")

    def _latest_ts(self) -> Optional[str]:
        try:
            files = [f for f in os.listdir(self.cam_dir) if f.endswith(".ts")]
            if not files:
                return None
            files.sort(key=lambda fn: os.path.getmtime(os.path.join(self.cam_dir, fn)))
            return os.path.join(self.cam_dir, files[-1])
        except OSError:
            return None

    def _update_file_status(self):
        """Refresh current_segment / last_size for the status UI. Informational only."""
        path = self._latest_ts()
        self.current_segment = os.path.basename(path) if path else None
        if path and os.path.exists(path):
            try:
                sz = os.path.getsize(path)
                if sz > self.last_size:
                    self.last_grow_time = time.monotonic()
                    self.last_size = sz
            except OSError:
                pass

    def _rtsp_stalled(self) -> bool:
        """True if RTSP ingest has not advanced past STALL_THRESHOLD_S (and grace passed)."""
        with self._lock:
            proc = self._proc
        if proc is None or proc.poll() is not None:
            return False  # caller handles dead-process case separately
        now = time.monotonic()
        if now - self._pipeline_start_mono < STALL_GRACE_AFTER_START_S:
            return False
        rchar = _read_rchar(proc.pid)
        if rchar is None:
            return False  # can't read /proc -> don't false-trigger
        if self._last_rchar is None or rchar > self._last_rchar:
            self._last_rchar = rchar
            self._last_rchar_growth_mono = now
            return False
        return (now - self._last_rchar_growth_mono) > STALL_THRESHOLD_S

    def _watch_loop(self):
        backoff = BACKOFF_START
        while not self._stop.is_set():
            free = self.disk_free_fn()
            if free is not None and free < MIN_FREE_DISK_MB:
                logger.error(f"[cam{self.index}] disk critically low ({free} MB), stopping recorder")
                self.last_error = f"Disk full (<{MIN_FREE_DISK_MB} MB free)"
                self._stop_pipeline()
                self.state = "disk_stopped"
                if self.on_disk_critical:
                    try:
                        self.on_disk_critical()
                    except Exception:
                        pass
                return

            with self._lock:
                proc = self._proc
            if proc is None or proc.poll() is not None:
                if self._stop.is_set():
                    break
                self.state = "restarting"
                self.restart_count += 1
                logger.info(f"[cam{self.index}] starting pipeline (attempt {self.restart_count})")
                ok = self._start_pipeline()
                if not ok:
                    self.last_error = "gst-launch failed to start"
                    time.sleep(backoff)
                    backoff = min(BACKOFF_MAX, backoff * 1.5)
                    continue
                backoff = BACKOFF_START

            self._update_file_status()

            if self._rtsp_stalled():
                logger.warning(
                    f"[cam{self.index}] rtsp ingest stalled (no rchar growth for "
                    f"{STALL_THRESHOLD_S:.0f}s), restarting pipeline"
                )
                self._stop_pipeline()
                self.last_size = 0
                self.last_grow_time = None
                self._last_rchar = None
                self.last_error = "rtsp stalled"

            time.sleep(WATCHDOG_INTERVAL_S)

        self._stop_pipeline()
        self.state = "stopped"

    def _start_pipeline(self) -> bool:
        self._stop_pipeline()
        cmd = self._build_cmd()
        logger.info(f"[cam{self.index}] {' '.join(cmd)}")
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
            )
        except Exception as e:
            logger.exception(f"[cam{self.index}] Popen failed: {e}")
            self._proc = None
            return False
        self._stderr_thread = threading.Thread(target=self._stderr_reader, daemon=True)
        self._stderr_thread.start()
        time.sleep(1.5)
        if self._proc.poll() is not None:
            err = self._proc.stderr.read().decode(errors="replace") if self._proc.stderr else ""
            logger.error(f"[cam{self.index}] died on start: {err}")
            self._proc = None
            return False
        self.state = "running"
        self._pipeline_start_mono = time.monotonic()
        self.last_grow_time = self._pipeline_start_mono
        self.last_size = 0
        self._last_rchar = None
        self._last_rchar_growth_mono = self._pipeline_start_mono
        return True

    def _stop_pipeline(self):
        with self._lock:
            proc = self._proc
            self._proc = None
        if proc and proc.poll() is None:
            try:
                proc.send_signal(signal.SIGINT)
                proc.wait(timeout=8)
            except Exception:
                try:
                    proc.kill()
                    proc.wait(timeout=2)
                except Exception:
                    pass

    def start(self):
        self._stop.clear()
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._watch_loop, daemon=True, name=f"rec-{self.index}")
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=15)
            self._thread = None

    def restart(self):
        self.last_size = 0
        self.last_grow_time = None
        self._last_rchar = None
        self._stop_pipeline()

    def status_dict(self) -> Dict[str, Any]:
        path = self._latest_ts()
        size_mb = 0.0
        if path and os.path.exists(path):
            size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
        total_mb = 0.0
        try:
            for fn in os.listdir(self.cam_dir):
                if fn.endswith(".ts"):
                    fp = os.path.join(self.cam_dir, fn)
                    total_mb += os.path.getsize(fp) / (1024 * 1024)
        except OSError:
            pass
        return {
            "index": self.index,
            "name": self.stream["name"],
            "stream_id": self.stream["stream_id"],
            "rtsp_url": self.stream["rtsp_url"],
            "state": self.state,
            "last_error": self.last_error,
            "gst_errors": self.gst_errors,
            "restart_count": self.restart_count,
            "current_segment": self.current_segment,
            "current_segment_mb": size_mb,
            "session_total_mb": round(total_mb, 2),
        }


class RecorderManager:
    def __init__(
        self,
        session_root: str,
        streams: List[Dict[str, Any]],
        segment_ns: int,
        disk_free_fn: Callable[[], Optional[float]],
        on_disk_critical: Optional[Callable[[], None]] = None,
        is_internal_sd: bool = True,
    ):
        self.session_root = session_root
        self.segment_ns = segment_ns
        self.disk_free_fn = disk_free_fn
        self.on_disk_critical = on_disk_critical
        self.is_internal_sd = is_internal_sd
        self.recorders: List[Recorder] = []
        for i, s in enumerate(streams):
            sub = _sanitize_dir_name(s["name"])
            cam_dir = os.path.join(session_root, f"cam_{i}_{sub}")
            self.recorders.append(
                Recorder(i, s, cam_dir, segment_ns, disk_free_fn, on_disk_critical=on_disk_critical)
            )

    def start_all(self):
        # Phase-offset the segment rotation boundaries. On internal SD we use a
        # much larger per-cam stagger so the four splitmuxsink close-and-fsync
        # bursts don't all land in the same wall-clock second. NVMe/SSD media
        # doesn't benefit (microsecond-scale write latency), so we keep the
        # minimal 0.5s stagger there.
        stagger = START_STAGGER_INTERNAL_SD_S if self.is_internal_sd else START_STAGGER_EXTERNAL_S
        if len(self.recorders) > 1:
            logger.info(
                f"Starting {len(self.recorders)} recorders with {stagger:.1f}s per-cam stagger "
                f"({'internal SD' if self.is_internal_sd else 'external media'})"
            )
        for i, r in enumerate(self.recorders):
            if i > 0:
                time.sleep(stagger)
            r.start()

    def stop_all(self):
        for r in self.recorders:
            r.stop()

    def restart_cam(self, index: int) -> bool:
        if 0 <= index < len(self.recorders):
            self.recorders[index].restart()
            return True
        return False

    def status(self) -> List[Dict[str, Any]]:
        return [r.status_dict() for r in self.recorders]
