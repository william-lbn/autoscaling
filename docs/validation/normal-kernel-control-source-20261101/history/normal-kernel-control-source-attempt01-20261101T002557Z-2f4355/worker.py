from pathlib import Path
import json,subprocess,time
d=Path(__file__).parent;t=time.monotonic()
with (d/'build.log').open('wb') as f:r=subprocess.run(json.loads((d/'command.json').read_text()),stdout=f,stderr=subprocess.STDOUT,timeout=300)
p=d/'receipt/test.rc';rc=int(p.read_text()) if p.exists() else None
v={'build_exit':r.returncode,'test_exit':rc,'pass':r.returncode==0 and rc==0,'seconds':time.monotonic()-t,'normal_authorized':False,'E':0};(d/'result.json').write_text(json.dumps(v,indent=2)+'\n');raise SystemExit(0 if v['pass'] else 1)
