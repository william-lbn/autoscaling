from pathlib import Path
import subprocess,json,os,datetime
p=Path(__file__).parent
raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout
env=os.environ.copy();env.update({k.decode():v.decode() for x in raw.split(b'\0') if x and b'=' in x for k,v in [x.split(b'=',1)]})
command=['docker','buildx','build','--builder','neon-data','--network','host','--allow','network.host','--progress','plain']
for key in ['HTTP_PROXY','HTTPS_PROXY','NO_PROXY','http_proxy','https_proxy','no_proxy']:command+=['--build-arg',key]
command+=['--build-arg','NONCE='+p.name,'--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
with (p/'run.log').open('wb') as log:r=subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT)
result={'build_exit':r.returncode,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
f=p/'export/test.exitcode'
if f.exists():result['test_exit']=int(f.read_text())
f=p/'export/source.exitcode'
if f.exists():result['source_exit']=int(f.read_text())
(p/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(r.returncode or result.get('test_exit',1))
