"""
Per-stream GStreamer RTSP -> MPEG-TS segment recorder with watchdog and restart.
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
STALL_THRESHOLD_S = 20.0
BACKOFF_START = 1.0
BACKOFF_MAX = 10.0


def _sanitize_dir_name(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_.-]+", "_", name.strip()) or "cam"
    return s[:80]


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

    def _segment_pattern(self) -> str:
        base = os.path.join(self.cam_dir, "seg_%05d.ts")
        return base

    def _build_cmd(self) -> List[str]:
        url = self.stream["rtsp_url"]
        loc = self._segment_pattern()
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
            "max-size-bytes=0",
            "max-size-time=1000000000",
            "!",
            "splitmuxsink",
            "muxer-factory=mpegtsmux",
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

            path = self._latest_ts()
            self.current_segment = os.path.basename(path) if path else None
            if path and os.path.exists(path):
                sz = os.path.getsize(path)
                if sz > self.last_size:
                    self.last_grow_time = time.monotonic()
                    self.last_size = sz
                elif self.last_grow_time is not None:
                    if time.monotonic() - self.last_grow_time > STALL_THRESHOLD_S:
                        logger.warning(f"[cam{self.index}] file stalled, restarting pipeline")
                        self._stop_pipeline()
                        self.last_size = 0
                        self.last_grow_time = None
                        self.last_error = "segment stalled"
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
        self.last_grow_time = time.monotonic()
        self.last_size = 0
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
    ):
        self.session_root = session_root
        self.segment_ns = segment_ns
        self.disk_free_fn = disk_free_fn
        self.on_disk_critical = on_disk_critical
        self.recorders: List[Recorder] = []
        for i, s in enumerate(streams):
            sub = _sanitize_dir_name(s["name"])
            cam_dir = os.path.join(session_root, f"cam_{i}_{sub}")
            self.recorders.append(
                Recorder(i, s, cam_dir, segment_ns, disk_free_fn, on_disk_critical=on_disk_critical)
            )

    def start_all(self):
        for r in self.recorders:
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
