docker exec extension-vshieblueosbrexplorehddvrmain python3 -c '
import urllib.request, json
for label, q in (("since=0", "0"), ("since=1.0e12", "1000000000000")):
    try:
        r = urllib.request.urlopen("http://127.0.0.1:4444/auto_download_zip/info?since=" + q, timeout=5)
        body = r.read().decode()
        print(label, "->", body)
    except Exception as e:
        print(label, "-> ERROR", repr(e))
try:
    r = urllib.request.urlopen("http://127.0.0.1:4444/status", timeout=5)
    s = json.loads(r.read())
    print("status.auto_download_enabled =", s.get("auto_download_enabled"))
    print("status.auto_download_interval_minutes =", s.get("auto_download_interval_minutes"))
    print("status.session_id =", s.get("session_id"))
    print("status.boot_stage =", s.get("boot_stage"))
    print("status.version =", s.get("version"))
    cams = s.get("cams") or []
    print("cams:", len(cams), "recording:", [c.get("recording") for c in cams])
except Exception as e:
    print("status -> ERROR", repr(e))
'
