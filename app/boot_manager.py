"""
Boot: wait for MCM to publish H264 RTSP streams so the cloud relay can start.

Cloud-only build — no disk recorder, no zip, no USB, no CPU calm gate. The
only thing gating "ready" is MCM answering `/streams` with at least one
H264 RTSP endpoint.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

from mcm_client import wait_for_streams

logger = logging.getLogger(__name__)

MCM_POLL_S = float(os.environ.get("MCM_POLL_INTERVAL_S", "1"))
MCM_MAX_WAIT_S = float(os.environ.get("MCM_MAX_WAIT_S", "60"))

StageCallback = Optional[Callable[[str], None]]
StreamsCallback = Optional[Callable[[List[Dict[str, Any]]], None]]


def run_boot_sequence(
    mcm_base: str,
    on_stage: StageCallback = None,
    on_streams: StreamsCallback = None,
) -> Tuple[List[Dict[str, Any]], Optional[str], str]:
    """Return (streams, boot_error, boot_stage).

    Stages:
      mcm_wait   — polling MCM /streams
      mcm_error  — MCM never returned any H264 RTSP streams
      ready      — streams available; caller has started cloud relay
    """
    def _stage(name: str) -> str:
        if on_stage:
            try:
                on_stage(name)
            except Exception:
                logger.exception("on_stage(%s) failed", name)
        return name

    boot_stage = _stage("mcm_wait")
    streams = wait_for_streams(base=mcm_base, poll_interval_s=MCM_POLL_S, max_wait_s=MCM_MAX_WAIT_S)
    if not streams:
        return (
            [],
            (
                "No H264 RTSP streams from MAVLink Camera Manager. "
                "Configure streams in BlueOS (Video Streams / MCM, port 6020)."
            ),
            _stage("mcm_error"),
        )

    if len(streams) < 4:
        logger.warning("Only %d H264 RTSP stream(s) from MCM (expected up to 4)", len(streams))

    if on_streams:
        try:
            on_streams(list(streams))
        except Exception:
            logger.exception("on_streams callback failed")

    return streams, None, _stage("ready")
