#!/usr/bin/env bash
# Quick version probe — hits whichever port Flask is on inside the container.
# Tries 4444 (new) then 6010 (legacy / pre-update BlueOS env override).
set -u
CN="extension-vshieblueosbrexplorehddvrmain"
docker exec "$CN" python3 - <<'PY'
import urllib.request, json
for port in (4444, 6010):
    try:
        r = urllib.request.urlopen(f"http://127.0.0.1:{port}/status", timeout=3)
        d = json.loads(r.read().decode())
        print(f"port={port} version={d.get('version')} recording={d.get('recording')}")
        break
    except Exception as e:
        print(f"port={port} ERR {e}")
PY
