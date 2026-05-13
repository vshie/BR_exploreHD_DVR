docker exec extension-vshieblueosbrexplorehddvrmain python3 -c '
import urllib.request, json, time
r = urllib.request.urlopen("http://127.0.0.1:6010/status", timeout=5)
s = json.loads(r.read())
print("version=", s.get("version"))
print("recording=", s.get("recording"))
print("boot_stage=", s.get("boot_stage"))
print("auto_download_enabled=", s.get("auto_download_enabled"))
print("segment_seconds=", s.get("segment_seconds"))
print("next_expected_close_epoch=", s.get("next_expected_close_epoch"))
print("now=", time.time())
cams = s.get("cams") or []
print("cams:", len(cams))
for c in cams:
    print("  ", c.get("name"), "state=", c.get("state"), "current_segment=", c.get("current_segment"), "first_epoch=", c.get("current_segment_first_epoch"))
'
