from pathlib import Path
import json,subprocess,os,time
p=Path(__file__).parent
raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout
env=os.environ.copy();env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in [x.split(b'=',1)]})
cmd=['docker','buildx','build','--builder','neon-data','--network','host','--allow','network.host','--progress','plain','--resource','memory=2g','--resource','memory-swap=2g','--resource','cpu-quota=200000','--build-arg','NONCE='+p.name,'--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
for k in ['HTTP_PROXY','HTTPS_PROXY','NO_PROXY','http_proxy','https_proxy','no_proxy']:cmd+=['--build-arg',k]
(p/'build-command.json').write_text(json.dumps(cmd,indent=2)+'\n');t=time.monotonic()
with(p/'compile.log').open('wb')as f:r=subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT)
(p/'compile-exit-code.txt').write_text(str(r.returncode)+'\n');(p/'compile-result.json').write_text(json.dumps({'exit':r.returncode,'seconds':time.monotonic()-t},indent=2)+'\n')
if r.returncode:raise SystemExit(r.returncode)
phase=int((p/'export/compile.exitcode').read_text());raise SystemExit(phase)
