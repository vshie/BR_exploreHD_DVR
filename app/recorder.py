"""
Per-stream GStreamer RTSP -> MPEG-TS segment recorder with watchdog and restart.

Stall isolation (v1.0.15 baseline, refined in v1.0.17):
  - A 60s RAM-backed queue sits between rtspsrc and splitmuxsink so SD fsync
    pauses up to ~60s are absorbed without backpressuring rtspsrc.
  - Segment rotations request a keyframe at roll time so the new file opens
    at a clean I-frame boundary without waiting for the next natural GOP.

v1.0.17 MCM-friendliness changes:
  - Stall threshold raised to 90s AND required to be observed across two
    consecutive watchdog windows before restarting. MCM 0.2.4's RTSP fanout
    gets wedged by rapid SETUP/TEARDOWN cycles, so we churn pipelines as
    rarely as possible and only when a stall is clearly persistent.
  - Staggered starts dropped. MCM handles a single batched SETUP burst while
    it is fresh more reliably than a trickled-in sequence (later clients were
    observed to starve after the first was already streaming).

v1.0.20 transport change:
  - Default rtspsrc transport is now UDP with TCP fallback (`udp+tcp`) instead
    of TCP-only. Even with WiFi fully disabled we continued to see periodic
    MCM-side producer glitches that manifest as multi-second flat-line stalls
    on TCP; UDP drops the affected packets and recovers instead of blocking.
    Overridable per-install via the DVR_RTSP_PROTOCOLS env var.

v1.0.22 stall-signal fix (the big one):
  - Prior versions used /proc/<pid>/io rchar as the stall signal. That worked
    for TCP RTSP (where rtspsrc's main task does the recv()) but SILENTLY does
    not count bytes for UDP RTSP: GStreamer's udpsrc element handles recvmsg()
    in a way Linux does not attribute to the task's rchar counter, so rchar
    stays flat at ~97 KB forever while video streams at 10 Mbps and the output
    file grows normally. Once we switched to udp+tcp in 1.0.20 this produced
    constant false-positive "rtsp stalled" watchdog restarts every 90s even
    though recording was healthy. The fix: watch the actual bytes written to
    disk across all segments in the session (monotonic, rollover-safe) instead.
    Proven with an isolated test where rchar=97 KB over 60s while the output
    file grew from 10 MB to 82 MB.
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

# Stall detection tracks the total bytes written across all segments in the
# current session (not /proc rchar — see the v1.0.22 note above for why rchar
# is unreliable with UDP rtspsrc). The 60s RAM queue ahead of splitmuxsink
# means a brief SD fsync pause does not reach the muxer's write() immediately,
# but the queue empties quickly once the SD bus recovers, so the aggregate
# file growth is still the right signal for "is data actually flowing through
# the pipeline". The threshold is generous because MCM 0.2.4 sometimes pauses
# briefly on cross-consumer negotiation (e.g. when Cockpit opens WebRTC) and
# we don't want to tear down our RTSP session and amplify the churn on MCM.
STALL_THRESHOLD_S = 90.0
# Require the stalled condition to persist across this many consecutive
# watchdog polls before we restart. With a 5s poll interval and a 90s
# threshold this means ~90s + (N-1)*5s of confirmed inactivity before we
# tear down and reconnect.
STALL_CONFIRM_POLLS = 2
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

# Per-cam start stagger. We keep a very small (0.5s) jitter so that the four
# splitmuxsink fsyncs don't land in exactly the same millisecond, but we no
# longer spread starts across many seconds: MCM 0.2.4's RTSP fanout was
# observed to serve the first client cleanly and starve subsequent clients
# that connect while the first is already PLAYING. Issuing all four SETUPs
# in a tight batch while MCM is fresh avoids that failure mode.
START_STAGGER_S = 0.5

# RTSP transport preference for rtspsrc. Accepts any value rtspsrc understands:
#   "tcp"         - interleave RTP over the RTSP TCP control connection (TCP HOL blocking applies)
#   "udp+tcp"     - negotiate UDP for RTP, fall back to TCP if UDP SETUP fails
#   "udp"         - UDP only (no fallback; fails cleanly if the server rejects UDP)
# UDP avoids TCP head-of-line blocking: a brief MCM-side producer glitch drops
# a few RTP packets and recovers instead of flat-lining the whole socket for
# tens of seconds like TCP does. Overridable without a rebuild via the extension
# env var DVR_RTSP_PROTOCOLS (BlueOS extension config).
RTSP_PROTOCOLS = os.environ.get("DVR_RTSP_PROTOCOLS", "udp+tcp").strip() or "udp+tcp"


def _sanitize_dir_name(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_.-]+", "_", name.strip()) or "cam"
    return s[:80]


def _read_session_bytes(cam_dir: str) -> Optional[int]:
    """Total bytes across all .ts segments in a cam's session directory.

    This is the authoritative "is the pipeline producing output" signal:
    it's independent of which specific segment file is currently being
    written (so a rollover doesn't look like a regression), it's unaffected
    by the /proc rchar udpsrc accounting quirk that broke v1.0.17..v1.0.21,
    and it directly observes what ends up on disk.
    """
    total = 0
    try:
        with os.scandir(cam_dir) as it:
            for entry in it:
                if entry.is_file() and entry.name.endswith(".ts"):
                    try:
                        total += entry.stat().st_size
                    except OSError:
                        pass
    except (FileNotFoundError, PermissionError):
        return None
    return total


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

        # Session-bytes tracking for stall detection. This is the monotonic
        # sum of all .ts file sizes in cam_dir; it advances every time
        # splitmuxsink writes data, regardless of which segment is active.
        self._last_session_bytes: Optional[int] = None
        self._last_bytes_growth_mono: float = 0.0
        # Number of consecutive watchdog polls that have seen the stall condition.
        # We only restart when this reaches STALL_CONFIRM_POLLS.
        self._stall_streak: int = 0

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
            f"protocols={RTSP_PROTOCOLS}",
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

    def _pipeline_stalled(self) -> bool:
        """True if total session bytes on disk have not grown past STALL_THRESHOLD_S (and grace passed)."""
        with self._lock:
            proc = self._proc
        if proc is None or proc.poll() is not None:
            return False  # caller handles dead-process case separately
        now = time.monotonic()
        if now - self._pipeline_start_mono < STALL_GRACE_AFTER_START_S:
            return False
        total = _read_session_bytes(self.cam_dir)
        if total is None:
            return False  # cam_dir missing -> don't false-trigger
        if self._last_session_bytes is None or total > self._last_session_bytes:
            self._last_session_bytes = total
            self._last_bytes_growth_mono = now
            return False
        return (now - self._last_bytes_growth_mono) > STALL_THRESHOLD_S

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
                logger.info(
                    f"[cam{self.index}] starting pipeline "
                    f"(attempt {self.restart_count}, rtsp_protocols={RTSP_PROTOCOLS})"
                )
                ok = self._start_pipeline()
                if not ok:
                    self.last_error = "gst-launch failed to start"
                    time.sleep(backoff)
                    backoff = min(BACKOFF_MAX, backoff * 1.5)
                    continue
                backoff = BACKOFF_START

            self._update_file_status()

            if self._pipeline_stalled():
                self._stall_streak += 1
                if self._stall_streak >= STALL_CONFIRM_POLLS:
                    logger.warning(
                        f"[cam{self.index}] pipeline stalled (no segment-bytes growth for "
                        f"~{STALL_THRESHOLD_S:.0f}s, confirmed across "
                        f"{self._stall_streak} polls), restarting pipeline"
                    )
                    self._stop_pipeline()
                    self.last_size = 0
                    self.last_grow_time = None
                    self._last_session_bytes = None
                    self._stall_streak = 0
                    self.last_error = "pipeline stalled"
                else:
                    logger.info(
                        f"[cam{self.index}] pipeline looks stalled "
                        f"(streak={self._stall_streak}/{STALL_CONFIRM_POLLS}); "
                        f"waiting for confirmation before restart"
                    )
            else:
                self._stall_streak = 0

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
        # Seed the session-bytes baseline at pipeline start so "growth" is measured
        # against whatever is already in the cam_dir (e.g. earlier segments from a
        # prior attempt in the same session), not against zero.
        self._last_session_bytes = _read_session_bytes(self.cam_dir)
        self._last_bytes_growth_mono = self._pipeline_start_mono
        self._stall_streak = 0
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
        self._last_session_bytes = None
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
        # Use a tight 0.5s per-cam stagger on both internal SD and external
        # media. Larger staggers were observed to interact badly with MCM
        # 0.2.4's RTSP server: later SETUPs issued while earlier clients were
        # already PLAYING starved of data. A near-batched start while MCM is
        # fresh gives the most reliable multi-consumer fanout.
        if len(self.recorders) > 1:
            logger.info(
                f"Starting {len(self.recorders)} recorders with {START_STAGGER_S:.1f}s per-cam stagger "
                f"({'internal SD' if self.is_internal_sd else 'external media'})"
            )
        for i, r in enumerate(self.recorders):
            if i > 0:
                time.sleep(START_STAGGER_S)
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
