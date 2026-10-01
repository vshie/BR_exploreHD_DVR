"""
Boot: wait for direct H.264 RTSP sources (QooCam) so the cloud relay can start.

qoocam branch — no MCM. Ready when at least one configured RTSP TCP endpoint
answers (camera powered and Live publishing).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from qoocam_control import POWER_CYCLE_HINT, rtsp_video_ready
from stream_sources import (
    list_direct_h264_rtsp_streams,
    qoocam_rtsp_url,
    wait_for_direct_streams,
)

logger = logging.getLogger(__name__)

SOURCE_POLL_S = float(os.environ.get("QOOCAM_POLL_INTERVAL_S", "2"))
SOURCE_MAX_WAIT_S = float(os.environ.get("QOOCAM_MAX_WAIT_S", "60"))
VIDEO_MAX_WAIT_S = float(os.environ.get("QOOCAM_VIDEO_WAIT_S", "90"))

StageCallback = Optional[Callable[[str], None]]
StreamsCallback = Optional[Callable[[List[Dict[str, Any]]], None]]


def run_boot_sequence(
    _unused_mcm_base: str = "",
    on_stage: StageCallback = None,
    on_streams: StreamsCallback = None,
) -> Tuple[List[Dict[str, Any]], Optional[str], str]:
    """Return (streams, boot_error, boot_stage).

    Stages:
      source_wait — polling configured RTSP hosts
      source_error — no RTSP endpoint answered in time, or it served no
        video within QOOCAM_VIDEO_WAIT_S (video_watchdog retries boot once
        the camera serves video)
      ready — camera answered DESCRIBE with video; caller has started cloud
        relay
    """
    def _stage(name: str) -> str:
        if on_stage:
            try:
                on_stage(name)
            except Exception:
                logger.exception("on_stage(%s) failed", name)
        return name

    boot_stage = _stage("source_wait")
    streams = wait_for_direct_streams(
        poll_interval_s=SOURCE_POLL_S,
        max_wait_s=SOURCE_MAX_WAIT_S,
    )
    if not streams:
        return (
            [],
            (
                f"No reachable QooCam RTSP at {qoocam_rtsp_url()}. "
                + POWER_CYCLE_HINT
            ),
            _stage("source_error"),
        )

    host = urlparse(streams[0]["rtsp_url"]).hostname
    if not host:
        return [], "Discovered QooCam RTSP URL has no host", _stage("source_error")
    # Do not restart the camera's encoder over OSC here. On this firmware
    # camera._startRtspLivePreview never answers and leaves 8554 open with
    # no video until the camera is power cycled. The camera's own Live
    # settings (3840x1920, ~10 Mbps) are what we want; just wait for video.
    deadline = time.monotonic() + VIDEO_MAX_WAIT_S
    while not rtsp_video_ready(host):
        if time.monotonic() >= deadline:
            return (
                [],
                "The QooCam is on the network but is not sending video. "
                + POWER_CYCLE_HINT,
                _stage("source_error"),
            )
        logger.info("QooCam RTSP is open but serves no video yet; waiting")
        time.sleep(SOURCE_POLL_S)

    logger.info(
        "Direct RTSP ready: %s",
        ", ".join(f"{s['name']}={s['rtsp_url']}" for s in streams),
    )

    if on_streams:
        try:
            on_streams(list(streams))
        except Exception:
            logger.exception("on_streams callback failed")

    return streams, None, _stage("ready")
