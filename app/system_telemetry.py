"""
System telemetry for BR_exploreHD_DVR: Pi CPU temperature, voltage, clock, time sync, disk space.
"""

import ctypes
import logging
import os
import subprocess
from datetime import datetime

logger = logging.getLogger(__name__)


def get_cpu_temperature():
    try:
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
            return round(int(f.read().strip()) / 1000.0, 1)
    except Exception:
        pass
    try:
        out = subprocess.check_output(
            ["vcgencmd", "measure_temp"], timeout=2, stderr=subprocess.DEVNULL
        ).decode()
        return float(out.split("=")[1].split("'")[0])
    except Exception as e:
        logger.debug(f"CPU temp read failed: {e}")
    return None


def get_cpu_voltage():
    try:
        out = subprocess.check_output(
            ["vcgencmd", "measure_volts", "core"], timeout=2, stderr=subprocess.DEVNULL
        ).decode()
        return float(out.split("=")[1].strip().rstrip("V"))
    except Exception:
        pass
    try:
        with open("/sys/devices/platform/soc/soc:firmware/get_throttled", "r") as f:
            throttled = int(f.read().strip(), 16)
        if throttled & 0x1:
            return "Under-voltage"
        return "OK"
    except Exception as e:
        logger.debug(f"CPU voltage read failed: {e}")
    return None


def get_cpu_clock_mhz():
    try:
        out = subprocess.check_output(
            ["vcgencmd", "measure_clock", "arm"], timeout=2, stderr=subprocess.DEVNULL
        ).decode()
        freq_hz = int(out.split("=")[1].strip())
        return round(freq_hz / 1_000_000, 0)
    except Exception:
        pass
    try:
        with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq", "r") as f:
            freq_khz = int(f.read().strip())
            return round(freq_khz / 1000, 0)
    except Exception as e:
        logger.debug(f"CPU clock read failed: {e}")
    return None


def _check_adjtimex():
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        buf = (ctypes.c_char * 256)()
        ctypes.memset(buf, 0, 256)
        result = libc.adjtimex(buf)
        if result == 5:
            return False
        if 0 <= result <= 4:
            return True
    except Exception as e:
        logger.debug(f"adjtimex check failed: {e}")
    return None


def is_time_synced():
    result = _check_adjtimex()
    if result is not None:
        return result
    try:
        out = subprocess.check_output(
            ["timedatectl", "show", "--property=NTPSynchronized"],
            timeout=2,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        return out == "NTPSynchronized=yes"
    except Exception:
        pass
    if os.path.exists("/run/systemd/timesync/synchronized"):
        return True
    if datetime.now().year >= 2025:
        return True
    if datetime.now().year < 2024:
        return False
    return None


def get_cpu_load_avg():
    try:
        with open("/proc/loadavg", "r") as f:
            return float(f.read().split()[0])
    except Exception as e:
        logger.debug(f"CPU load read failed: {e}")
    return None


def get_disk_free_mb(path="/app/recordings"):
    try:
        stat = os.statvfs(path)
        return round((stat.f_bavail * stat.f_frsize) / (1024 * 1024), 1)
    except Exception as e:
        logger.debug(f"Disk free check failed: {e}")
    return None


def get_system_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_all_telemetry(recording_ok=None, usb_disk_free_mb=None):
    return {
        "cpu_temp_c": get_cpu_temperature(),
        "cpu_voltage_v": get_cpu_voltage(),
        "cpu_clock_mhz": get_cpu_clock_mhz(),
        "cpu_load_avg": get_cpu_load_avg(),
        "time_synced": is_time_synced(),
        "system_time": get_system_time(),
        "disk_free_mb": get_disk_free_mb(),
        "usb_disk_free_mb": usb_disk_free_mb,
        "recording_ok": recording_ok,
    }
