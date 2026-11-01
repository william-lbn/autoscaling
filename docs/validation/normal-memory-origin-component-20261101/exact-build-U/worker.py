from pathlib import Path
import json,subprocess,time
d=Path(__file__).parent;t=time.monotonic();cmd=json.loads((d/'command.json').read_text())
with (d/'build.log').open('wb') as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,timeout=600)
get=lambda n:int((d/'receipt'/n).read_text().strip()) if (d/'receipt'/n).exists() else None
a,b=get('test.rc'),get('compile.rc')
v={'build_exit':r.returncode,'test_exit':a,'compile_exit':b,'pass':r.returncode==0 and a==0 and b==0,'seconds':time.monotonic()-t,'normal_qualified':False,'E':0}
(d/'result.json').write_text(json.dumps(v,indent=2)+'\n')
raise SystemExit(0 if v['pass'] else 1)
