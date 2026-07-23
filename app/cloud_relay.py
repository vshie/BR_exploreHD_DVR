"""
RTMP cloud relay: spawn one ffmpeg subprocess per MCM RTSP camera that copies
the H.264 elementary stream to a hardcoded RTMP/FLV endpoint, with a watchdog
that respawns dead pipes on a backoff. Audio is dropped (`-an`) — the cameras
don't carry useful audio and the receiving service is video-only.

Equivalent shell command per cam:

    ffmpeg -rtsp_transport tcp -stimeout 5000000 -i <rtsp_url_from_mcm> \\
           -c:v copy -an -f flv rtmp://35.83.28.160/live/bom_cam0N

Design notes
------------

  - Per-cam Relay object owns a Popen and a watchdog thread; a RelayManager
    orchestrates start/stop for the whole fleet.
  - RTSP transport is **TCP** (RTP-over-RTSP-interleaved), not UDP.
    Every cloud-relay ffmpeg reads MCM's RTSP over loopback
    (`rtsp://127.0.0.1:8554/...`) at the same time as the browser's
    WebRTC Live view is reading the same streams — up to eight
    concurrent RTSP consumers on 127.0.0.1. On loopback, TCP costs
    effectively nothing (no physical medium, no HOL blocking of a
    real link, memcpy-only) while UDP can drop RTP packets at the
    kernel receive buffer under that concurrency before anything
    ever leaves the vessel. TCP eliminates that class of on-box
    loss for the price of a slightly larger per-packet framing.
  - `-stimeout 5000000` (5 s, in microseconds) makes ffmpeg exit
    promptly when the RTSP source goes silent (Wi-Fi blip, MCM stall),
    instead of hanging on the kernel's much longer TCP timeouts.
    See `RTSP_RW_TIMEOUT_FLAG` below for the ffmpeg-version-compat
    notes — the bundled image ships ffmpeg 4.x where this option is
    `-stimeout`, not `-rw_timeout`.
  - Reconnect strategy: short sleep with 0..2 s of jitter after a
    healthy run; exponential backoff (5 → 10 → 20 → 40 → 60 s, capped)
    when ffmpeg keeps dying within HEALTHY_RUN_S. Jitter prevents all
    four cams from reconnecting on the exact same tick after a common
    disturbance and slamming the receiver.
  - Separate (longer) backoff schedule kicks in when the upstream RTMP
    server reports `Already publishing`; see STALE_PUBLISHER_BACKOFF_*
    below.
  - The destination RTMP server URL is HARDCODED per the project
    requirement that there is no per-deployment knob for the RTMP
    destination — only an enable/disable toggle.
  - The per-cam stream KEY is derived from the stream's NAME as
    configured in BlueOS/MCM: the number embedded in the name selects
    the `bom_camNN` bucket (e.g. a stream named "... 5" publishes to
    `bom_cam05`). This lets two vessels whose cameras are numbered 1-4
    and 5-8 respectively publish to distinct, non-colliding keys on the
    shared receiver — without any per-deployment configuration. Streams
    whose name has no number fall back to the legacy positional map
    (list index 0..3 → `bom_cam01..bom_cam04`).
"""

from __future__ import annotations

import logging
import os
import random
import re
import signal
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Hardcoded cloud destination. The RTMP server URL never varies between
# installs.
RTMP_BASE_URL = "rtmp://35.83.28.160/live"

# The per-cam stream KEY is `<prefix><NN>` where NN is the number parsed
# from the stream's BlueOS/MCM name, zero-padded to RTMP_STREAM_KEY_PAD
# digits. A stream named "... 5" → `bom_cam05`; two vessels numbered 1-4
# and 5-8 therefore publish to disjoint key sets on the shared receiver.
RTMP_STREAM_KEY_PREFIX = "bom_cam"
RTMP_STREAM_KEY_PAD = 2

# Legacy positional fallback, used ONLY for streams whose name has no
# parseable number: list index 0..3 → bom_cam01..bom_cam04.
RTMP_STREAM_KEYS: List[str] = ["bom_cam01", "bom_cam02", "bom_cam03", "bom_cam04"]

# Matches a run of digits anywhere in the stream name. We take the LAST
# match (see `_stream_number_from_name`) so names like "exploreHD 5",
# "cam5", "1080p Front 5" all resolve to camera number 5.
_STREAM_NUMBER_RE = re.compile(r"\d+")

# Watchdog cadence and respawn backoff. The values below were chosen to
# match the receiving RTMP service's published recommendations:
#
#   * Healthy run (>= HEALTHY_RUN_S) followed by an exit → retry quickly
#     with `BACKOFF_START`. This is the "vessel Wi-Fi blipped, come back
#     fast" case: one short sleep and we re-publish.
#   * Short-lived run (< HEALTHY_RUN_S) → grow backoff exponentially up
#     to `BACKOFF_MAX`, so a truly down network doesn't slam the server
#     with hundreds of empty publishes per minute.
#
# Schedule on continuous failure: 5, 10, 20, 40, 60, 60, ... (capped).
# A `RECONNECT_JITTER_S` of 0..2 s of uniform jitter is added on top of
# every wait so all four cams don't reconnect on the same tick after a
# common disturbance (Wi-Fi outage, MCM restart, etc.).
WATCHDOG_INTERVAL_S = 2.0
BACKOFF_START = 5.0
BACKOFF_MAX = 60.0
BACKOFF_GROWTH = 2.0
RECONNECT_JITTER_S = 2.0

# How long after spawn we consider the pipeline "established" enough that
# an exit should reset the backoff. A pipeline that runs for >= this many
# seconds is treated as healthy; anything shorter than that and we keep
# growing the backoff.
HEALTHY_RUN_S = 30.0

# RTSP socket I/O timeout passed to ffmpeg so that a dead RTSP link
# (MCM stopped publishing, camera unplugged) makes ffmpeg exit within a
# few seconds instead of hanging on the kernel's much longer TCP
# timeout. Value is microseconds.
# 5 s is short enough that the watchdog notices outages quickly, long
# enough that an unloaded but slow link doesn't false-trip.
#
# IMPORTANT: option-name compatibility across ffmpeg majors.
#
#   * ffmpeg 4.x — the version shipped by Ubuntu 22.04's `apt install
#     ffmpeg` and therefore the one bundled in the BlueOS extension
#     image — exposes the RTSP-demuxer socket I/O timeout as
#     `-stimeout` (microseconds). It does NOT recognise `-rw_timeout`
#     and aborts with `Option rw_timeout not found.` before it even
#     opens the input, which is impossible to distinguish from a
#     legitimate connect-timeout in our logs.
#   * ffmpeg 5.x+ renamed `-stimeout` to `-timeout` on the RTSP
#     demuxer (the old `-stimeout` form is kept as a deprecated alias
#     for now) and added a generic `-rw_timeout` AVOption usable on
#     any URL context.
#
# We standardise on `-stimeout` because (a) it is the only flag
# accepted by the ffmpeg version we actually ship today, and (b) it is
# still recognised (deprecated alias) by ffmpeg 5.x, so a future base
# image bump won't silently break this. If/when we move to ffmpeg 6+
# and `-stimeout` is removed entirely, swap to `-timeout` here.
RTSP_RW_TIMEOUT_FLAG = "-stimeout"
RTSP_RW_TIMEOUT_US = 5_000_000

# Special-case backoff for the RTMP "Already publishing" reject path. When
# our previous ffmpeg dies without cleanly tearing down its RTMP socket
# (network glitch, abrupt MCM RTSP close, container restart, etc.), the
# upstream RTMP server keeps the stale publisher's stream-key slot held
# for its keepalive timeout — typically 5..90 s depending on which RTMP
# server is running and how it's configured — and refuses any new
# `publish` command for the same key with `Server error: Already
# publishing`. The regular 1..10 s respawn backoff is too short for the
# slow end of that range (we thrash and the cam stays stuck), but a
# fixed long wait penalises the much more common fast-recovery case
# (SRS/MediaMTX/Wowza all clear publisher slots within 5..15 s by
# default).
#
# Strategy: probe early, back off exponentially, cap at 90 s. We start
# at 5 s, double on each consecutive "Already publishing" reject, and
# clamp at 90 s. Schedule: 5, 10, 20, 40, 80, 90, 90, ... The counter
# resets to zero the moment a pipeline runs healthily for HEALTHY_RUN_S
# seconds, so a transient blip doesn't poison the next disconnect.
STALE_PUBLISHER_BACKOFF_INITIAL_S = 5.0
STALE_PUBLISHER_BACKOFF_MAX_S = 90.0

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


def _stream_number_from_name(name: Optional[str]) -> Optional[int]:
    """Extract the camera number from a BlueOS/MCM stream name.

    Returns the LAST run of digits in the name as an int (so "exploreHD 5",
    "cam5", and "1080p Front 5" all yield 5), or None if the name contains
    no digits. Taking the last run avoids being fooled by resolution/codec
    tokens that may precede the operator's camera number.
    """
    if not name:
        return None
    matches = _STREAM_NUMBER_RE.findall(str(name))
    if not matches:
        return None
    try:
        return int(matches[-1])
    except (TypeError, ValueError):
        return None


def _stream_key_for_number(n: int) -> str:
    return f"{RTMP_STREAM_KEY_PREFIX}{n:0{RTMP_STREAM_KEY_PAD}d}"


def _stream_key_for_index(cam_index: int) -> Optional[str]:
    if 0 <= cam_index < len(RTMP_STREAM_KEYS):
        return RTMP_STREAM_KEYS[cam_index]
    return None


def _stream_key_for_stream(cam_index: int, stream: Dict[str, Any]) -> Optional[str]:
    """Pick the RTMP stream key for a cam.

    Preference order:
      1. The number parsed from the stream's BlueOS-configured NAME, e.g.
         a stream named "... 5" → bom_cam05. This is the primary path and
         is what lets a vessel with cameras named 5-8 publish to
         bom_cam05..bom_cam08.
      2. Legacy positional fallback (list index 0..3 → bom_cam01..bom_cam04),
         used only when the name has no number.
    """
    n = _stream_number_from_name((stream or {}).get("name"))
    if n is not None and n > 0:
        return _stream_key_for_number(n)
    return _stream_key_for_index(cam_index)


def _rtmp_url_for_key(key: Optional[str]) -> Optional[str]:
    if not key:
        return None
    return f"{RTMP_BASE_URL}/{key}"


class Relay:
    """Single-cam RTMP relay (RTSP in, RTMP out, video-copy, no re-encode)."""

    def __init__(self, index: int, stream: Dict[str, Any]):
        self.index = index
        self.stream = stream
        self.stream_key = _stream_key_for_stream(index, stream) or ""
        self.rtmp_url = _rtmp_url_for_key(self.stream_key) or ""

        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None
        # Rolling buffer of the last few stderr lines, used as
        # post-mortem evidence when ffmpeg dies inside the
        # `_start_pipeline` 1 s probe. Without this, the dedicated
        # stderr reader thread wins the race against the post-mortem
        # `proc.stderr.read()`, the read returns empty, and the relay
        # surfaces a useless "ffmpeg exited immediately" message — as
        # happened on a real device when an option-name mismatch
        # (`-rw_timeout` vs `-stimeout`) caused every spawn to die
        # before opening the input. A short ring is enough: ffmpeg's
        # fatal output is usually one or two lines.
        self._stderr_recent: List[str] = []
        self._stderr_recent_max: int = 8
        self._stderr_recent_lock = threading.Lock()

        self.state = "idle"
        self.last_error = ""
        self.ff_errors = 0
        self.restart_count = 0
        self.started_at: Optional[float] = None
        self._pipeline_start_mono: float = 0.0

        # Set by `_stderr_reader` when ffmpeg's stderr matches one of the
        # `_STALE_PUBLISHER_PATTERNS`. The watchdog reads this flag on
        # subprocess exit to decide whether to apply the
        # stale-publisher exponential backoff instead of the regular
        # 1..10 s backoff. Always cleared just before each new spawn so
        # a fresh stale-publisher hit produces a fresh decision.
        self._stale_publisher_seen: bool = False
        # Counter of consecutive stale-publisher rejects since the last
        # healthy run. Doubles the wait time per reject (capped at
        # STALE_PUBLISHER_BACKOFF_MAX_S) so we probe early when the
        # server clears slots quickly (e.g. SRS @ 5 s) and stretch out
        # only when it doesn't (e.g. nginx-rtmp without
        # drop_idle_publisher). Reset to 0 on any HEALTHY_RUN_S+ run.
        self._stale_publisher_consecutive: int = 0
        # Cumulative count of stale-publisher waits across the lifetime
        # of this Relay. Surfaced via status_dict so the UI can render
        # "waiting for server to release stream key (Nth time)" rather
        # than a confusing growing restart_count.
        self.stale_publisher_waits: int = 0

    def _build_cmd(self) -> List[str]:
        url = self.stream["rtsp_url"]
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            # TCP for the local RTSP hop. See module docstring for why:
            # with 4 cloud-relay ffmpegs + up to 4 WebRTC readers all
            # reading MCM over 127.0.0.1, UDP can drop RTP packets at
            # the kernel socket buffer before they even leave the box.
            # Loopback TCP has essentially zero cost and eliminates
            # that class of on-box loss.
            "-rtsp_transport", "tcp",
            # RTSP socket I/O timeout. See RTSP_RW_TIMEOUT_FLAG above
            # for why this is `-stimeout` (ffmpeg 4.x) and not
            # `-rw_timeout` (5.x+). Without this, a dead uplink can
            # keep the subprocess alive for tens of seconds, delaying
            # the watchdog's reconnect cycle.
            RTSP_RW_TIMEOUT_FLAG, str(RTSP_RW_TIMEOUT_US),
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
                # Always push into the rolling ring buffer first, so the
                # post-mortem path in `_start_pipeline` can surface a
                # useful last_error even when the line didn't match an
                # explicit error/failed pattern. (ffmpeg's "Option X
                # not found." aborts go through stderr at info level.)
                with self._stderr_recent_lock:
                    self._stderr_recent.append(text)
                    if len(self._stderr_recent) > self._stderr_recent_max:
                        del self._stderr_recent[
                            : len(self._stderr_recent) - self._stderr_recent_max
                        ]
                low = text.lower()
                # Detect the RTMP server holding our previous publisher
                # session. We set the flag (don't sleep here — the
                # watchdog handles backoff after exit) and tag the error
                # message so the UI shows a useful explanation instead
                # of a bare "Already publishing".
                if any(p in low for p in _STALE_PUBLISHER_PATTERNS):
                    self._stale_publisher_seen = True
                    self.ff_errors += 1
                    next_wait = min(
                        STALE_PUBLISHER_BACKOFF_MAX_S,
                        STALE_PUBLISHER_BACKOFF_INITIAL_S
                        * (2 ** self._stale_publisher_consecutive),
                    )
                    self.last_error = (
                        f"RTMP server still holds the previous publisher "
                        f"session for {self.stream_key!r}; will retry in "
                        f"{int(next_wait)}s (consecutive stale rejects: "
                        f"{self._stale_publisher_consecutive + 1}). "
                        f"ffmpeg said: {text[:200]}"
                    )
                    logger.warning(
                        f"[relay{self.index}] stale RTMP publisher: {text}"
                    )
                elif "error" in low or "failed" in low or "not found" in low:
                    # `not found` catches "Option X not found." which
                    # ffmpeg prints at info level — that lone line is
                    # the difference between knowing what killed us
                    # and getting a useless generic exit message.
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
        # Clear the rolling stderr buffer so the post-mortem path
        # only reports lines from THIS attempt's ffmpeg, not stale
        # output from a prior failed spawn.
        with self._stderr_recent_lock:
            self._stderr_recent.clear()
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
            # Don't try to read stderr here — the dedicated reader
            # thread (started above) is already draining it, and
            # racing for the same FD just yields an empty buffer (the
            # original cause of every cam reporting "ffmpeg exited
            # immediately" with no diagnosis on v1.0.35). Give the
            # reader a moment to finish, then snapshot the ring
            # buffer it built up.
            if self._stderr_thread:
                self._stderr_thread.join(timeout=1.0)
            with self._stderr_recent_lock:
                tail = list(self._stderr_recent)
            msg = (
                "; ".join(tail)
                if tail
                else "ffmpeg exited immediately (no stderr captured)"
            )
            logger.error(f"[relay{self.index}] died on start: {msg}")
            self.last_error = msg[:500]
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
        # `backoff` is the *base* delay before the next respawn, ignoring
        # jitter. It starts at BACKOFF_START, grows by BACKOFF_GROWTH on
        # every short-lived run, caps at BACKOFF_MAX, and resets to
        # BACKOFF_START whenever a run lives at least HEALTHY_RUN_S.
        backoff = BACKOFF_START
        # `first_iter` lets the very first ffmpeg spawn happen
        # immediately when the relay is first enabled — operators
        # toggling Cloud ON expect to see streaming start right away,
        # not after a 5..7 s wait. Subsequent respawns always go through
        # the backoff+jitter path so a flapping link doesn't slam the
        # receiver.
        first_iter = True
        while not self._stop.is_set():
            with self._lock:
                proc = self._proc
            if proc is not None and proc.poll() is None:
                # ffmpeg is alive — just tick.
                time.sleep(WATCHDOG_INTERVAL_S)
                continue
            if self._stop.is_set():
                break

            # ffmpeg has died (or hasn't been started yet). Decide
            # whether the next respawn deserves a quick or slow retry
            # based on how long the *previous* run lived. Capture the
            # last spawn's start time then immediately invalidate it,
            # so a series of spawn failures (where _start_pipeline
            # never reaches the point of setting _pipeline_start_mono)
            # doesn't keep reading a stale long-ago healthy timestamp
            # and falsely classifying every retry as "healthy".
            had_recent_start = self._pipeline_start_mono > 0
            ran_for = (
                time.monotonic() - self._pipeline_start_mono
                if had_recent_start
                else 0.0
            )
            self._pipeline_start_mono = 0.0
            ran_healthy = had_recent_start and ran_for >= HEALTHY_RUN_S
            if not first_iter and ran_healthy:
                # Long-lived run → this looks like a transient blip.
                # Reset the backoff *before* using it so the first
                # post-healthy sleep is BACKOFF_START (5 s), not the
                # grown value left over from a prior failure streak.
                backoff = BACKOFF_START
                # A healthy run also clears any stale-publisher
                # streak — the next stale reject we see is treated
                # as the first one, so we probe early again.
                self._stale_publisher_consecutive = 0

            # If the just-died ffmpeg saw "Already publishing" on
            # stderr, the upstream RTMP server is still holding our
            # previous publisher slot. Use the dedicated stale-publisher
            # backoff (5..90 s exponential) instead of the regular
            # respawn cadence — the slot won't free up just by
            # reconnecting, only by waiting out the server's keepalive
            # timeout. The wait uses `_stop.wait` so a UI toggle-off /
            # shutdown / settings change interrupts immediately rather
            # than sitting through the full sleep.
            if self._stale_publisher_seen:
                n = self._stale_publisher_consecutive
                wait_s = min(
                    STALE_PUBLISHER_BACKOFF_MAX_S,
                    STALE_PUBLISHER_BACKOFF_INITIAL_S * (2 ** n),
                )
                wait_s += random.uniform(0, RECONNECT_JITTER_S)
                self._stale_publisher_consecutive = n + 1
                self.stale_publisher_waits += 1
                self.state = "waiting_for_rtmp_release"
                logger.warning(
                    f"[relay{self.index}] RTMP server still publishing; "
                    f"probing again in {wait_s:.1f}s "
                    f"(consecutive stale rejects: "
                    f"{self._stale_publisher_consecutive}, "
                    f"lifetime waits: {self.stale_publisher_waits})"
                )
                if self._stop.wait(wait_s):
                    break
                # Reset the regular short-backoff so once the slot
                # opens the very next attempt isn't unnecessarily
                # delayed on top of the stale-publisher wait.
                backoff = BACKOFF_START
                self._stale_publisher_seen = False
            elif not first_iter:
                # Regular respawn path. Sleep the *current* `backoff`
                # plus 0..2 s of uniform jitter, then grow `backoff`
                # for next time iff this run was short-lived. So a
                # streak of short-lived runs sleeps 5, 10, 20, 40, 60
                # (capped) on consecutive deaths — matching the
                # receiver operator's recommended schedule. Jitter
                # prevents all four cams from reconnecting on exactly
                # the same tick after a common disturbance (Wi-Fi
                # outage, MCM restart, container reboot).
                wait_s = backoff + random.uniform(0, RECONNECT_JITTER_S)
                self.state = "restarting"
                logger.info(
                    f"[relay{self.index}] ffmpeg exited after "
                    f"{ran_for:.1f}s; respawning in {wait_s:.1f}s "
                    f"(backoff base {backoff:.0f}s, attempt "
                    f"{self.restart_count + 1})"
                )
                if self._stop.wait(wait_s):
                    break
                if not ran_healthy:
                    backoff = min(BACKOFF_MAX, backoff * BACKOFF_GROWTH)

            first_iter = False
            self.state = "restarting"
            self.restart_count += 1
            logger.info(
                f"[relay{self.index}] starting ffmpeg "
                f"(attempt {self.restart_count})"
            )
            ok = self._start_pipeline()
            if not ok:
                if self.state == "skipped":
                    return
                self.state = "error"
                # Loop continues; next iteration sees proc is None or
                # exited and applies the grown backoff before retrying.
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
        next_wait_s = min(
            STALE_PUBLISHER_BACKOFF_MAX_S,
            STALE_PUBLISHER_BACKOFF_INITIAL_S
            * (2 ** self._stale_publisher_consecutive),
        )
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
            "stale_publisher_consecutive": self._stale_publisher_consecutive,
            "stale_publisher_next_wait_s": int(next_wait_s),
            "rtsp_rw_timeout_us": RTSP_RW_TIMEOUT_US,
            "started_at": self.started_at,
        }


class RelayManager:
    """Owns a Relay per stream and orchestrates start/stop."""

    def __init__(self, streams: List[Dict[str, Any]]):
        self.relays: List[Relay] = []
        for i, s in enumerate(streams):
            self.relays.append(Relay(i, s))
        # Surface name→key collisions loudly: two MCM streams whose names
        # resolve to the same number (or two number-less streams sharing a
        # positional fallback key) would both publish to the same bom_camNN
        # bucket and fight over the RTMP publisher slot ("Already
        # publishing" forever). This is an operator misconfiguration in
        # BlueOS, so we log it rather than silently remap.
        seen: Dict[str, int] = {}
        for r in self.relays:
            if not r.stream_key:
                continue
            if r.stream_key in seen:
                logger.error(
                    "Cloud relay: stream key %r is claimed by both cam %d "
                    "(%r) and cam %d (%r) — both will publish to the same "
                    "RTMP bucket and collide. Rename the streams in BlueOS "
                    "so each maps to a distinct number.",
                    r.stream_key,
                    seen[r.stream_key],
                    self.relays[seen[r.stream_key]].stream.get("name"),
                    r.index,
                    r.stream.get("name"),
                )
            else:
                seen[r.stream_key] = r.index

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
        # Actual per-cam keys in use (name-derived), falling back to the
        # legacy positional list when no relay manager is running yet.
        "stream_keys": (
            [c.get("stream_key") for c in cams] if cams else list(RTMP_STREAM_KEYS)
        ),
        "cams": cams,
        "streaming_count": streaming,
        "waiting_for_rtmp_release_count": waiting,
        "total_count": len(cams),
        "total_restarts": total_restarts,
        "total_stale_publisher_waits": total_stale_waits,
        "stale_publisher_backoff_initial_s": int(
            STALE_PUBLISHER_BACKOFF_INITIAL_S
        ),
        "stale_publisher_backoff_max_s": int(STALE_PUBLISHER_BACKOFF_MAX_S),
        "backoff_start_s": int(BACKOFF_START),
        "backoff_max_s": int(BACKOFF_MAX),
        "reconnect_jitter_s": float(RECONNECT_JITTER_S),
        "rtsp_rw_timeout_us": RTSP_RW_TIMEOUT_US,
        "healthy_run_s": int(HEALTHY_RUN_S),
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
        "stale_publisher_backoff_initial_s": st[
            "stale_publisher_backoff_initial_s"
        ],
        "stale_publisher_backoff_max_s": st[
            "stale_publisher_backoff_max_s"
        ],
        "rtmp_base_url": st["rtmp_base_url"],
    }
