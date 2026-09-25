"""
Direct RTSP sources for the qoocam branch (no MCM / BlueOS Video Streams).

BlueOS dnsmasq on eth0 serves 192.168.2.101–200 with 24h leases. The QooCam
IP can therefore change across camera or Pi boots. Discovery order:

1. ``QOOCAM_RTSP_URL`` if set (explicit override)
2. neighbor/ARP match for ``QOOCAM_MAC`` (default: this camera's MAC)
3. ARP match for Kandao OUI ``70:65:a3``
4. TCP scan of the BlueOS DHCP pool on port 8554, confirming ``Server: QooCam``

The stream ``name`` must embed a camera number so cloud_relay maps it to
``bom_camNN`` (e.g. "QooCam 9" → bom_cam09).
"""

from __future__ import annotations

import logging
import os
import socket
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DEFAULT_QOOCAM_STREAM_NAME = "QooCam 9"
DEFAULT_QOOCAM_MAC = "70:65:a3:11:36:b0"
KANDAO_OUI = "70:65:a3"
RTSP_PORT = 8554
# BlueOS eth0 dnsmasq: --dhcp-range=192.168.2.101,192.168.2.200,...
DHCP_POOL_FIRST = 101
DHCP_POOL_LAST = 200
DHCP_POOL_PREFIX = "192.168.2."

_last_good_url: Optional[str] = None


def _norm_mac(mac: str) -> str:
    return mac.strip().lower().replace("-", ":")


def _arp_neighbors() -> List[Tuple[str, str]]:
    """Return (ip, mac) from the kernel neighbor table."""
    path = "/proc/net/arp"
    out: List[Tuple[str, str]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            next(f, None)
            for line in f:
                parts = line.split()
                if len(parts) < 4:
                    continue
                ip, mac = parts[0], _norm_mac(parts[3])
                if mac in ("00:00:00:00:00:00", ""):
                    continue
                out.append((ip, mac))
    except OSError as e:
        logger.debug("Could not read %s: %s", path, e)
    return out


def _rtsp_is_qoocam(host: str, port: int = RTSP_PORT, timeout: float = 1.5) -> bool:
    """True if host:port speaks RTSP and identifies as QooCam."""
    req = (
        f"OPTIONS rtsp://{host}:{port}/ RTSP/1.0\r\n"
        "CSeq: 1\r\n"
        "User-Agent: br-dvr-qoocam\r\n"
        "\r\n"
    ).encode("ascii")
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall(req)
            buf = b""
            while len(buf) < 1024:
                chunk = s.recv(512)
                if not chunk:
                    break
                buf += chunk
                if b"\r\n\r\n" in buf:
                    break
    except OSError:
        return False
    text = buf.decode("ascii", errors="replace")
    if "RTSP/1.0 200" not in text:
        return False
    return "qoocam" in text.lower()


def _rtsp_tcp_open(rtsp_url: str, timeout: float = 2.0) -> bool:
    try:
        parsed = urlparse(rtsp_url)
        host = parsed.hostname
        if not host:
            return False
        port = parsed.port or 554
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError as e:
        logger.debug("RTSP probe failed for %s: %s", rtsp_url, e)
        return False


def _url_for_host(host: str) -> str:
    return f"rtsp://{host}:{RTSP_PORT}/"


def _explicit_url() -> Optional[str]:
    raw = (os.environ.get("QOOCAM_RTSP_URL") or "").strip()
    return raw or None


def _target_mac() -> Optional[str]:
    raw = (os.environ.get("QOOCAM_MAC") or DEFAULT_QOOCAM_MAC).strip()
    return _norm_mac(raw) if raw else None


def discover_qoocam_rtsp_url(*, scan_dhcp_pool: bool = False) -> Optional[str]:
    """Resolve the live QooCam RTSP URL; IP may change with DHCP."""
    global _last_good_url

    explicit = _explicit_url()
    if explicit:
        return explicit

    if _last_good_url and _rtsp_is_qoocam(
        urlparse(_last_good_url).hostname or "",
        urlparse(_last_good_url).port or RTSP_PORT,
    ):
        return _last_good_url

    neighbors = _arp_neighbors()
    want = _target_mac()
    candidates: List[str] = []
    if want:
        candidates.extend(ip for ip, mac in neighbors if mac == want)
    candidates.extend(
        ip for ip, mac in neighbors
        if mac.startswith(KANDAO_OUI) and ip not in candidates
    )

    for ip in candidates:
        if _rtsp_is_qoocam(ip):
            url = _url_for_host(ip)
            logger.info("Discovered QooCam RTSP at %s (ARP)", url)
            _last_good_url = url
            return url

    if not scan_dhcp_pool:
        return None

    for last in range(DHCP_POOL_FIRST, DHCP_POOL_LAST + 1):
        ip = f"{DHCP_POOL_PREFIX}{last}"
        if ip in candidates:
            continue
        if not _rtsp_is_qoocam(ip, timeout=0.25):
            continue
        url = _url_for_host(ip)
        logger.info("Discovered QooCam RTSP at %s (DHCP pool scan)", url)
        _last_good_url = url
        return url

    return None


def _configured_sources(*, scan_dhcp_pool: bool = False) -> List[Dict[str, str]]:
    names = (os.environ.get("QOOCAM_STREAM_NAME") or DEFAULT_QOOCAM_STREAM_NAME).strip()
    name_list = [n.strip() for n in names.split(",") if n.strip()]
    url = discover_qoocam_rtsp_url(scan_dhcp_pool=scan_dhcp_pool)
    if not url:
        return []
    name = name_list[0] if name_list else DEFAULT_QOOCAM_STREAM_NAME
    return [{"name": name, "rtsp_url": url}]


def list_direct_h264_rtsp_streams(
    require_reachable: bool = True,
    *,
    scan_dhcp_pool: bool = False,
) -> List[Dict[str, Any]]:
    """Return normalized stream dicts compatible with cloud_relay / /streams API."""
    out: List[Dict[str, Any]] = []
    for i, src in enumerate(_configured_sources(scan_dhcp_pool=scan_dhcp_pool)):
        url = src["rtsp_url"]
        reachable = _rtsp_tcp_open(url)
        if require_reachable and not reachable:
            continue
        out.append(
            {
                "stream_id": f"qoocam-{i}",
                "name": src["name"],
                "rtsp_url": url,
                "webrtc_page": None,
                "mcm_root": None,
                "running": reachable,
                "source": "direct",
            }
        )
    return out


def wait_for_direct_streams(
    poll_interval_s: float = 2.0,
    max_wait_s: float = 60.0,
) -> List[Dict[str, Any]]:
    """Poll until the QooCam RTSP is discovered and reachable, or timeout."""
    deadline = time.monotonic() + max_wait_s
    last: List[Dict[str, Any]] = []
    while time.monotonic() < deadline:
        last = list_direct_h264_rtsp_streams(
            require_reachable=True, scan_dhcp_pool=True
        )
        if last:
            return last
        logger.info(
            "Waiting for QooCam RTSP (MAC %s or DHCP pool %s%d-%d); retry in %.1fs",
            _target_mac() or "(any Kandao)",
            DHCP_POOL_PREFIX,
            DHCP_POOL_FIRST,
            DHCP_POOL_LAST,
            poll_interval_s,
        )
        time.sleep(poll_interval_s)
    return last
