"""Keep video flowing from the QooCam, or tell the operator how to fix it.

The camera can stall with its RTSP port still open: OPTIONS answers, DESCRIBE
never does, so MediaMTX times out forever while the boot stage says "ready".
This watchdog uses MediaMTX's own "stream is available" signal as ground truth.
When video has been missing for ``QOOCAM_STALL_S`` it:

1. pauses MediaMTX so the camera has no RTSP client, then probes DESCRIBE
   directly; if the camera serves video, MediaMTX is resumed;
2. otherwise reports ``stalled`` with instructions
   to power cycle the camera at its breaker, and keeps probing every
   ``QOOCAM_PROBE_S`` so video resumes on its own after the power cycle.

If boot failed because the camera never answered, the watchdog re-runs boot
as soon as the camera serves video.

It never restarts the camera's encoder over OSC: on this firmware
``camera._startRtspLivePreview`` is what leaves the camera stalled.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse

import local_preview
from qoocam_control import POWER_CYCLE_HINT, rtsp_video_ready
from stream_sources import qoocam_rtsp_url, rtsp_tcp_open

logger = logging.getLogger(__name__)

TICK_S = 5.0
STALL_S = float(os.environ.get("QOOCAM_STALL_S", "45"))
PROBE_S = float(os.environ.get("QOOCAM_PROBE_S", "15"))
# Direct probes that succeed while MediaMTX keeps failing before we give up.
MAX_RECOVERIES = 3

_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_state = "waiting"
_message = ""
_since = time.monotonic()
_power_cycle_needed = False


def _set(state: str, message: str = "") -> None:
    global _state, _message, _since, _power_cycle_needed
    with _lock:
        if state != _state:
            logger.info("Video watchdog: %s -> %s %s", _state, state, message)
            _since = time.monotonic()
        _state = state
        _message = message
        _power_cycle_needed = state in ("stalled", "unreachable")


def _restart_clock() -> None:
    global _since
    with _lock:
        _since = time.monotonic()


def status() -> Dict[str, Any]:
    with _lock:
        return {
            "state": _state,
            "message": _message,
            "power_cycle_needed": _power_cycle_needed,
            "offline_s": round(local_preview.offline_seconds(), 1),
            "state_age_s": round(time.monotonic() - _since, 1),
        }


def _camera_host() -> str:
    return urlparse(qoocam_rtsp_url()).hostname or ""


def _stuck_message(host: str) -> tuple[str, str]:
    if rtsp_tcp_open(qoocam_rtsp_url()):
        return (
            "stalled",
            f"The QooCam at {host} is on the network but is not sending video. "
            + POWER_CYCLE_HINT,
        )
    return (
        "unreachable",
        f"The QooCam at {host} is not answering on RTSP. " + POWER_CYCLE_HINT,
    )


class _Watchdog:
    def __init__(self, get_stage: Callable[[], str], boot_busy: Callable[[], bool],
                 retry_boot: Callable[[], None]) -> None:
        self.get_stage = get_stage
        self.boot_busy = boot_busy
        self.retry_boot = retry_boot
        self.recoveries = 0
        self.next_probe = 0.0

    def _probe_direct(self, host: str) -> bool:
        """Probe the camera with MediaMTX paused so we are its only client."""
        local_preview.pause()
        time.sleep(2.0)
        return rtsp_video_ready(host)

    def _recover(self, host: str) -> None:
        _set("recovering", "Video stopped; reconnecting to the QooCam.")
        if self.recoveries < MAX_RECOVERIES and self._probe_direct(host):
            self.recoveries += 1
            logger.info("QooCam serves video directly; resuming MediaMTX")
            local_preview.resume()
            _restart_clock()
            return
        self._enter_stuck(host)

    def _enter_stuck(self, host: str) -> None:
        local_preview.pause()
        state, message = _stuck_message(host)
        _set(state, message)
        self.next_probe = time.monotonic() + PROBE_S

    def _probe_stuck(self, host: str) -> None:
        if time.monotonic() < self.next_probe:
            return
        self.next_probe = time.monotonic() + PROBE_S
        if rtsp_video_ready(host):
            logger.info("QooCam is serving video again; resuming MediaMTX")
            self.recoveries = 0
            _set("recovering", "QooCam is back; reconnecting video.")
            local_preview.resume()
        else:
            state, message = _stuck_message(host)
            _set(state, message)

    def _boot_failed(self, host: str) -> None:
        if time.monotonic() < self.next_probe:
            return
        self.next_probe = time.monotonic() + PROBE_S
        local_preview.pause()
        if rtsp_video_ready(host):
            logger.info("QooCam is serving video; re-running boot")
            self.recoveries = 0
            _set("recovering", "QooCam is back; restarting video.")
            self.retry_boot()
        else:
            state, message = _stuck_message(host)
            _set(state, message)

    def tick(self) -> None:
        if self.boot_busy():
            return
        host = _camera_host()
        stage = self.get_stage()
        if stage in ("source_error", "error"):
            self._boot_failed(host)
            return
        if stage != "ready":
            _set("waiting", "Waiting for boot to finish.")
            return

        if local_preview.video_online():
            if _state != "ok":
                _set("ok")
            self.recoveries = 0
            return

        if _state in ("stalled", "unreachable"):
            self._probe_stuck(host)
            return

        if _state == "recovering":
            waited = time.monotonic() - _since
        else:
            waited = local_preview.offline_seconds()
            if _state != "no_video":
                _set("no_video", "Waiting for video from the QooCam.")
        if waited >= STALL_S:
            self._recover(host)

    def run(self) -> None:
        while True:
            try:
                self.tick()
            except Exception:
                logger.exception("Video watchdog tick failed")
            time.sleep(TICK_S)


def start(get_stage: Callable[[], str], boot_busy: Callable[[], bool],
          retry_boot: Callable[[], None]) -> None:
    global _thread
    if _thread is not None:
        return
    _thread = threading.Thread(
        target=_Watchdog(get_stage, boot_busy, retry_boot).run,
        daemon=True,
        name="video-watchdog",
    )
    _thread.start()
