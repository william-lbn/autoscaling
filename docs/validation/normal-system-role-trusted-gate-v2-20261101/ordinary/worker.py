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
phase=int((p/'export/test.exitcode').read_text());(p/'component-stage-exitcode.txt').write_text(str(phase)+'\\n')
if phase:raise SystemExit(phase)
# 沿原缓存genisoimage工具；仅创建本任务纯6资产ISO，不挂载Host。
image='sha256:cb6635e1790c7ceae132b6a75a61e85e33be1fde8dbd9c421a17a33cd2494b9d'
cmd=['docker','run','--rm','--network','none','--memory','512m','--memory-swap','512m','--cpus','1','-v',str(p)+':/proof:rw','--entrypoint','genisoimage',image,'-quiet','-J','-R','-o','/proof/runtime.iso','/proof/export/fixture/isofiles']
with(p/'iso.log').open('wb')as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
(p/'iso-command.json').write_text(json.dumps(cmd,indent=2)+'\n');(p/'iso.exitcode').write_text(str(r.returncode)+'\n')
if r.returncode:raise SystemExit(r.returncode)
with(p/'native-driver.log').open('wb')as f:r=subprocess.run(['python3',str(p/'run-kvm.py')],stdout=f,stderr=subprocess.STDOUT)
raise SystemExit(r.returncode)
