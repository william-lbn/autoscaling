from pathlib import Path
import subprocess,json,datetime,time
p=Path('/data/neon-build/validation/normal-force-built-source-20261101T045225Z-413336')
cmd=json.loads((p/"command.json").read_text())
start=time.monotonic()
with(p/"build.log").open("wb") as log:
 r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
result={"exit":r.returncode,"seconds":time.monotonic()-start,"ended_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"scope":"READONLY_SOURCE_EXPORT_NOT_PROOF","U":0,"S":0,"normal_authorized":False}
(p/"result.json").write_text(json.dumps(result,indent=2)+"\n")
raise SystemExit(r.returncode)
