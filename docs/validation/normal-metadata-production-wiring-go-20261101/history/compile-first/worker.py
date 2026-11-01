from pathlib import Path
import json,subprocess,time,datetime
p=Path(__file__).parent;t=time.monotonic()
with p.joinpath("build.log").open("wb") as f:x=subprocess.run(json.loads(p.joinpath("command.json").read_text()),stdout=f,stderr=subprocess.STDOUT)
rc=p.joinpath("export/compile.rc");z={"build_exit":x.returncode,"compile_exit":int(rc.read_text())if rc.exists()else None,"seconds":time.monotonic()-t,"end_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"U":0,"S":0,"normal_authorized":False,"E":0};p.joinpath("result.json").write_text(json.dumps(z,indent=2)+"\n");raise SystemExit(0 if z["build_exit"]==0 and z["compile_exit"]==0 else 1)
