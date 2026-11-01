from pathlib import Path
import subprocess,json,os,time,datetime
p=Path(__file__).parent;start=time.monotonic();env=dict(os.environ)
raw=subprocess.run(['/bin/bash','-c','set -a\nsource /data/neon-build/config/env.sh >/dev/null\nenv -0'],capture_output=True,check=True).stdout
allowed={'PATH','DOCKER_CONFIG','DOCKER_HOST','DOCKER_CONTEXT','BUILDX_BUILDER','HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy','NO_PROXY','no_proxy'}
env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in [x.split(b'=',1)]if k.decode()in allowed})
for k in ['HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy']:assert env.get(k)=='http://127.0.0.1:7890'
probe=subprocess.run(['docker','buildx','version'],env=env,capture_output=True,text=True);(p/'docker-buildx-version.json').write_text(json.dumps({'exit':probe.returncode,'stdout':probe.stdout,'stderr':probe.stderr},indent=2)+'\n');assert probe.returncode==0
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=2g','--resource','memory-swap=2g','--resource','cpu-quota=200000','--network','host','--allow','network.host','--progress','plain']
for k in ['HTTP_PROXY','HTTPS_PROXY','NO_PROXY','http_proxy','https_proxy','no_proxy']:cmd+=['--build-arg',k]
cmd+=['--build-arg','TOOLS_BASE=127.0.0.1:5001/neon/ordinary-v2-tools-base@sha256:bd15f43fe3c7fc4e3c09b8cab3595ec33b25ed8a7a4d9b47ee0d23556b0a9886','--build-arg','NONCE='+p.name,'--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')];(p/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with(p/'build.log').open('w')as log:rc=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=300).returncode
result={'build_exit':rc,'seconds':time.monotonic()-start,'Source':False,'normal':False,'E':0};f=p/'export/compile.exitcode';result['compile_exit']=int(f.read_text())if f.exists()else None;(p/'result.json').write_text(json.dumps(result,indent=2)+'\n');raise SystemExit(rc or result['compile_exit']or 0)
