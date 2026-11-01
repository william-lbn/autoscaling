from pathlib import Path
import subprocess,os,json,datetime,time
p=Path(__file__).parent
raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout
env=os.environ.copy();env.update({k.decode():v.decode() for x in raw.split(b'\0') if x and b'=' in x for k,v in [x.split(b'=',1)]})
c=['docker','buildx','build','--builder','neon-data','--network','host','--allow','network.host','--progress','plain','--resource','memory=2g','--resource','memory-swap=2g','--resource','cpu-quota=200000']
for k in ['HTTP_PROXY','HTTPS_PROXY','NO_PROXY','http_proxy','https_proxy','no_proxy']:c+=['--build-arg',k]
c+=['--build-arg','NONCE='+p.name,'--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
t=time.monotonic()
with (p/'run.log').open('wb') as log:r=subprocess.run(c,env=env,stdout=log,stderr=subprocess.STDOUT)
a={'build_exit':r.returncode,'seconds':time.monotonic()-t,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
f=p/'export/test.exitcode'
if f.exists():a['test_exit']=int(f.read_text())
(p/'result.json').write_text(json.dumps(a,indent=2)+'\n')
raise SystemExit(r.returncode or a.get('test_exit',1))
