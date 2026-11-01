from pathlib import Path
import subprocess,json,os,time,datetime
p=Path(__file__).parent;start=time.monotonic();env=dict(os.environ);proxy='http://127.0.0.1:7890';env.update(HTTP_PROXY=proxy,HTTPS_PROXY=proxy,http_proxy=proxy,https_proxy=proxy,NO_PROXY='localhost,127.0.0.1,::1',no_proxy='localhost,127.0.0.1,::1')
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=3g','--resource','memory-swap=3g','--resource','cpu-quota=200000','--network','host','--allow','network.host','--progress','plain']
for k in ['HTTP_PROXY','HTTPS_PROXY','NO_PROXY','http_proxy','https_proxy','no_proxy']:cmd+=['--build-arg',k]
cmd+=['--build-arg','TOOLS_BASE=127.0.0.1:5001/neon/ordinary-v2-tools-base@sha256:bd15f43fe3c7fc4e3c09b8cab3595ec33b25ed8a7a4d9b47ee0d23556b0a9886','--build-arg','NONCE='+p.name,'--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')];(p/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with(p/'build.log').open('w')as log:rc=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=300).returncode
result={'build_exit':rc,'seconds':time.monotonic()-start,'Source':False,'normal':False,'E':0};f=p/'export/compile.exitcode';result['compile_exit']=int(f.read_text())if f.exists()else None;(p/'result.json').write_text(json.dumps(result,indent=2)+'\n');raise SystemExit(rc or result['compile_exit']or 0)
