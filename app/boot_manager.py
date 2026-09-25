"""
Boot: wait for direct H.264 RTSP sources (QooCam) so the cloud relay can start.

qoocam branch — no MCM. Ready when at least one configured RTSP TCP endpoint
answers (camera powered and Live publishing).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

from stream_sources import wait_for_direct_streams

logger = logging.getLogger(__name__)

SOURCE_POLL_S = float(os.environ.get("QOOCAM_POLL_INTERVAL_S", "2"))
SOURCE_MAX_WAIT_S = float(os.environ.get("QOOCAM_MAX_WAIT_S", "60"))

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
      source_error — no RTSP endpoint answered in time
      ready — streams available; caller has started cloud relay
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
                "No reachable QooCam RTSP on the BlueOS ethernet DHCP pool "
                "(192.168.2.101–200:8554). Confirm Live is running, or set "
                "QOOCAM_RTSP_URL / QOOCAM_MAC."
            ),
            _stage("source_error"),
        )

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
