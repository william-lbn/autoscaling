from pathlib import Path
import subprocess,json,datetime,hashlib
d=Path(__file__).parent;r=Path('/data/neon-build/src/autoscaling');m=json.loads((d/'metadata.json').read_text())
for n,h in m['source']['owned5'].items():assert hashlib.sha256((r/n).read_bytes()).hexdigest()==h,n
cmd=['python3',str(r/'neonvm-kernel/profiles/normal-split-metadata-v1/build.py'),'--output',str(d/'build')];(d/'command.json').write_text(json.dumps(cmd,indent=2)+chr(10))
with(d/'unit.log').open('wb')as log:p=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
(d/'pipeline-result.json').write_text(json.dumps({'exit':p.returncode,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'normal_authorized':False,'E':0},indent=2)+chr(10))
raise SystemExit(p.returncode)
