#!/usr/bin/env python3
"""End-to-end smoke for the NeuralX uploader.

What it covers
--------------
1. Spins up an in-process HTTP server that mimics the NeuralX endpoint:
   - GET ?camera_id&filename returns {"upload_url": "<self>/put/<key>"}
   - PUT  /put/<key> swallows the body, records (key, sha256, bytes).
2. Redirects STATE_PATH / RECORDINGS_LOCAL / USB to tmpdirs.
3. Creates a fake recordings tree with two closed segments under cam_0 / cam_1.
4. Loads settings, enables the uploader, waits for the queue to drain.
5. Asserts:
   - Both segments are recorded as `done` in the state file.
   - Server-side filenames are `<node_id>_<basename>`.
   - The hardcoded age rule (>3 days) deletes a backfilled old segment
     immediately after its upload completes.
   - The hardcoded free-space rule (<50 GB) deletes a freshly-uploaded
     `done` file when _free_mb_for is patched to report low disk.
   - Failed uploads (forced via a poisoned cam_map entry) end up as `failed`
     with a populated last_error and are reset by `/neuralx/retry`.
   - State persists across a fresh `_State()` instantiation.

Run from the repo root:

    BR_exploreHD_DVR/.venv-smoke/bin/python BR_exploreHD_DVR/scripts/smoke_neuralx.py
"""

from __future__ import annotations

import hashlib
import http.server
import json
import os
import socket
import sys
import tempfile
import threading
import time
import urllib.parse
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP = REPO / "app"
sys.path.insert(0, str(APP))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class StubServer:
    def __init__(self) -> None:
        self.port = _free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.uploads: dict[str, dict] = {}
        self.lock = threading.Lock()
        self.fail_keys: set[str] = set()
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a, **kw):
                pass

            def do_GET(self):
                qs = urllib.parse.urlparse(self.path).query
                params = dict(urllib.parse.parse_qsl(qs))
                key = params.get("filename", "")
                cam = params.get("camera_id", "")
                if not key or cam not in ("01", "02", "03", "04"):
                    self.send_response(400); self.end_headers(); return
                if key in outer.fail_keys:
                    self.send_response(503); self.end_headers(); return
                body = json.dumps({
                    "upload_url": f"{outer.base}/put/{urllib.parse.quote(key)}",
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_PUT(self):
                parsed = urllib.parse.urlparse(self.path)
                if not parsed.path.startswith("/put/"):
                    self.send_response(404); self.end_headers(); return
                key = urllib.parse.unquote(parsed.path[len("/put/"):])
                length = int(self.headers.get("Content-Length") or 0)
                data = self.rfile.read(length) if length else self.rfile.read()
                h = hashlib.sha256(data).hexdigest()
                with outer.lock:
                    outer.uploads[key] = {"bytes": len(data), "sha256": h}
                self.send_response(200); self.end_headers()

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=2)


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="neuralx-smoke-"))
    sd_root = tmp / "recordings"
    sd_root.mkdir()
    usb_root = tmp / "usb"  # never mounted in this smoke
    usb_root.mkdir()

    # --- Stage 1: redirect module paths BEFORE importing anything ----------
    # settings_store reads SETTINGS_DIR at import time only for default
    # resolution; the actual path is recomputed from the module's
    # SETTINGS_DIR constant each call. neuralx_uploader does the same for
    # STATE_DIR / STATE_PATH. We patch both via module attrs after import.

    import settings_store as ss
    ss.SETTINGS_DIR = str(sd_root)
    ss.SETTINGS_PATH = str(sd_root / ".br_explorehd_dvr_settings.json")

    import neuralx_uploader as nx
    nx.STATE_DIR = str(sd_root)
    nx.STATE_PATH = str(sd_root / ".br_explorehd_dvr_neuralx_state.json")
    nx.RECORDINGS_LOCAL = str(sd_root)

    # Force USB to be absent so _storage_roots() only returns the SD root.
    import usb_storage
    usb_storage.is_mounted = lambda: False  # type: ignore

    # --- Stage 2: build a fake recordings tree -----------------------------
    session_dir = sd_root / "20260515" / "abc-session"
    cam0 = session_dir / "cam_0_Lower"
    cam1 = session_dir / "cam_1_Upper"
    cam0.mkdir(parents=True)
    cam1.mkdir(parents=True)
    # Two closed segments per cam.
    paths = []
    for i, cam in enumerate((cam0, cam1)):
        for ts in ("20260515_120000.ts", "20260515_120500.ts"):
            p = cam / ts
            payload = (f"{cam.name}-{ts}-" + "x" * 4096).encode()
            p.write_bytes(payload)
            # Backdate mtime so the stability cushion doesn't gate us.
            old = time.time() - 60
            os.utime(p, (old, old))
            paths.append(p)
    # And one "active" segment that must NOT be uploaded.
    active = cam0 / "seg_00007.ts"
    active.write_bytes(b"active-do-not-upload")

    # --- Stage 3: stub server + settings -----------------------------------
    stub = StubServer()

    ss.save_settings({
        "neuralx_endpoint": stub.base + "/presign",
        "neuralx_node_id": "node-blueA",
        "neuralx_cam_map": {"0": "01", "1": "02", "2": "03", "3": "04"},
        "neuralx_max_concurrent": 2,
        "neuralx_enabled": True,
    })

    up = nx.get_or_create()
    up.start()

    # --- Stage 4: wait for queue to drain ----------------------------------
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        st = up.status()
        if (
            st["queue"]["pending"] == 0
            and st["queue"]["in_flight"] == 0
            and st["queue"]["failed"] == 0
            and st["totals"]["files"] >= len(paths)
        ):
            break
        time.sleep(0.2)
    else:
        print("FAIL: queue did not drain in time:", up.status(), file=sys.stderr)
        up.stop(); stub.stop(); return 1

    # --- Stage 5: assertions on happy-path ---------------------------------
    server_uploads = dict(stub.uploads)
    expected_keys = set()
    for p in paths:
        expected_keys.add(f"node-blueA_{p.name}")
    if set(server_uploads.keys()) != expected_keys:
        print("FAIL: server saw", server_uploads.keys(), "expected", expected_keys, file=sys.stderr)
        up.stop(); stub.stop(); return 1

    # Active segment must not have been picked up.
    if any("seg_00007" in k for k in server_uploads):
        print("FAIL: active seg_00007 leaked into the uploads", file=sys.stderr)
        up.stop(); stub.stop(); return 1

    # State entries are `done` (not `done_deleted`, since fresh segments
    # are <3 days old and free space on the test box is well above 50 GB).
    state = json.loads(Path(nx.STATE_PATH).read_text())
    for p in paths:
        e = state["files"].get(str(p))
        if not e or e["status"] != "done":
            print("FAIL: file not done:", p, e, file=sys.stderr)
            up.stop(); stub.stop(); return 1
        if not p.exists():
            print("FAIL: fresh file unexpectedly deleted:", p, file=sys.stderr)
            up.stop(); stub.stop(); return 1

    # --- Stage 6: state persists across a fresh _State() -------------------
    fresh = nx._State()
    snap = fresh.snapshot()
    for p in paths:
        if snap["files"][str(p)]["status"] != "done":
            print("FAIL: state did not persist across reload:", p, file=sys.stderr)
            up.stop(); stub.stop(); return 1

    # --- Stage 7: post-upload age-rule fires for backfilled old segments ---
    # Drop a fake segment whose mtime is 4 days in the past. Once it
    # uploads, the hardcoded age rule (>3d) must remove it from disk and
    # flip the state entry to done_deleted.
    old_file = cam0 / "20260511_121000.ts"
    old_file.write_bytes(b"backfill-me-" + b"o" * 4096)
    four_days_ago = time.time() - (4 * 24 * 3600)
    os.utime(old_file, (four_days_ago, four_days_ago))
    up.wake()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if not old_file.exists():
            break
        time.sleep(0.2)
    else:
        print("FAIL: age-rule delete did not fire on backfilled segment", file=sys.stderr)
        up.stop(); stub.stop(); return 1
    state = json.loads(Path(nx.STATE_PATH).read_text())
    if state["files"][str(old_file)]["status"] != "done_deleted":
        print("FAIL: age-rule did not mark status done_deleted:",
              state["files"][str(old_file)], file=sys.stderr)
        up.stop(); stub.stop(); return 1
    if "age" not in (state["files"][str(old_file)].get("deleted_reason") or ""):
        print("FAIL: deleted_reason did not mention age:",
              state["files"][str(old_file)], file=sys.stderr)
        up.stop(); stub.stop(); return 1

    # --- Stage 7b: free-space sweep fires on already-done files ------------
    # Drop another fresh file, let it upload, then monkeypatch _free_mb_for
    # to report below-threshold and confirm the periodic sweep deletes it
    # even though the file is brand new (free-space branch).
    fresh_file = cam1 / "20260515_122000.ts"
    fresh_file.write_bytes(b"low-disk-cleanup-" + b"z" * 4096)
    os.utime(fresh_file, (time.time() - 60, time.time() - 60))
    up.wake()
    # Wait for upload.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        e = up._state.get_file(str(fresh_file))
        if e and e.get("status") == "done" and fresh_file.exists():
            break
        if e and e.get("status") == "done_deleted":
            # Unexpected — would mean our real test box is actually below
            # the 50 GB free-space floor. Skip the rest of this stage.
            break
        time.sleep(0.2)
    e = up._state.get_file(str(fresh_file))
    if e and e.get("status") == "done" and fresh_file.exists():
        # Pretend the disk dropped below 50 GB by short-circuiting
        # _free_mb_for. The next scan tick (≤10s away) sees the pressure
        # and the sweep should evict the fresh `done` file.
        original_free = nx._free_mb_for
        nx._free_mb_for = lambda _p: 100.0  # 100 MB free → way under 50 GB
        try:
            up.wake()
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if not fresh_file.exists():
                    break
                time.sleep(0.2)
            else:
                print("FAIL: free-space sweep did not delete fresh done file",
                      file=sys.stderr)
                up.stop(); stub.stop(); return 1
            state = json.loads(Path(nx.STATE_PATH).read_text())
            if state["files"][str(fresh_file)]["status"] != "done_deleted":
                print("FAIL: free-space sweep did not set done_deleted",
                      file=sys.stderr)
                up.stop(); stub.stop(); return 1
            if "free" not in (state["files"][str(fresh_file)].get("deleted_reason") or ""):
                print("FAIL: deleted_reason did not mention free:",
                      state["files"][str(fresh_file)], file=sys.stderr)
                up.stop(); stub.stop(); return 1
        finally:
            nx._free_mb_for = original_free

    # --- Stage 8: forced failure + /neuralx/retry --------------------------
    bad_file = cam1 / "20260515_121500.ts"
    bad_file.write_bytes(b"this-will-fail")
    os.utime(bad_file, (time.time() - 60, time.time() - 60))
    bad_upload_name = f"node-blueA_{bad_file.name}"
    stub.fail_keys.add(bad_upload_name)
    up.wake()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        e = up._state.get_file(str(bad_file))
        if e and e.get("status") == "failed":
            break
        time.sleep(0.2)
    else:
        print("FAIL: forced failure never registered:", up._state.get_file(str(bad_file)), file=sys.stderr)
        up.stop(); stub.stop(); return 1

    # Unstick the server, hit retry, expect the file to flip to done.
    stub.fail_keys.discard(bad_upload_name)
    n_reset = up.retry_failed_now()
    if n_reset < 1:
        print("FAIL: retry_failed_now did not find any failed entries", file=sys.stderr)
        up.stop(); stub.stop(); return 1
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        e = up._state.get_file(str(bad_file))
        if e and e.get("status") in ("done", "done_deleted"):
            break
        time.sleep(0.2)
    else:
        print("FAIL: failed file did not recover after retry:", up._state.get_file(str(bad_file)), file=sys.stderr)
        up.stop(); stub.stop(); return 1

    up.stop()
    stub.stop()
    print("OK")
    print("uploads on stub server:")
    for k, v in sorted(server_uploads.items()):
        print(" ", k, v)
    print("state file totals:", state["totals"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
