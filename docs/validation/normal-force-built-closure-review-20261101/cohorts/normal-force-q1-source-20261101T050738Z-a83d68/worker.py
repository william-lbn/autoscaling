from pathlib import Path
import json,subprocess,time,datetime
p=Path('/data/neon-build/validation/normal-force-q1-source-20261101T050738Z-a83d68')
t=time.monotonic()
with p.joinpath("build.log").open("wb") as f:r=subprocess.run(json.loads(p.joinpath("command.json").read_text()),stdout=f,stderr=subprocess.STDOUT)
p.joinpath("result.json").write_text(json.dumps({"exit":r.returncode,"seconds":time.monotonic()-t,"end_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"U":0,"S":0,"normal_authorized":False,"E":0},indent=2)+"\n")
raise SystemExit(r.returncode)
