from pathlib import Path
import subprocess,os,json,datetime
p=Path(__file__).parent
raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout
env=os.environ.copy();env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in[x.split(b'=',1)]})
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=3g','--resource','memory-swap=3g','--resource','cpu-quota=300000','--network','none','--progress','plain','--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
with(p/'run.log').open('wb')as log:r=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
result={'build_exit':r.returncode,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'normal_authorized':False,'S':0,'E':0}
for n in ['test','cli','darwin']:
 f=p/'export'/(n+'.rc');result[n+'_exit']=int(f.read_text())if f.exists()else None
(p/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(0 if r.returncode==0 and all(result[n+'_exit']==0 for n in ['test','cli','darwin']) else 1)
