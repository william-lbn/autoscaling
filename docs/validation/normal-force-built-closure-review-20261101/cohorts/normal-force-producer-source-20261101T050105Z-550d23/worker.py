from pathlib import Path
import json,subprocess,time,datetime
p=Path('/data/neon-build/validation/normal-force-producer-source-20261101T050105Z-550d23')
cmd=json.loads((p/"command.json").read_text())
start=time.monotonic()
with(p/"build.log").open("wb") as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
(p/"result.json").write_text(json.dumps({"exit":r.returncode,"seconds":time.monotonic()-start,"ended_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"U":0,"S":0,"normal_authorized":False},indent=2)+"\n")
raise SystemExit(r.returncode)
