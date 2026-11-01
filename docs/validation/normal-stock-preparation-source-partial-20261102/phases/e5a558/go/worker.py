from pathlib import Path
import json,subprocess,os,time
p=Path(__file__).parent
raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout
env=os.environ.copy();env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in [x.split(b'=',1)]})
cmd=['docker','buildx','build','--builder','neon-data','--network','host','--allow','network.host','--progress','plain','--resource','memory=3g','--resource','memory-swap=3g','--resource','cpu-quota=300000','--build-arg','NONCE='+p.name,'--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
for k in ['HTTP_PROXY','HTTPS_PROXY','NO_PROXY','http_proxy','https_proxy','no_proxy']:cmd+=['--build-arg',k]
(p/'build-command.json').write_text(json.dumps(cmd,indent=2)+'\n');t=time.monotonic()
with(p/'compile.log').open('wb')as f:r=subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT)
(p/'compile-exit-code.txt').write_text(str(r.returncode)+'\n');(p/'compile-result.json').write_text(json.dumps({'exit':r.returncode,'seconds':time.monotonic()-t},indent=2)+'\n')
if r.returncode:raise SystemExit(r.returncode)


phase_names=['u.exitcode','cli.exitcode','builder.exitcode','darwin.exitcode','native-compile.exitcode','host-compile.exitcode','host-export.exitcode','fixture-seed.exitcode']
phases={n:int((p/'export'/n).read_text())if(p/'export'/n).exists()else None for n in phase_names}
(p/'phase-results.json').write_text(json.dumps(phases,indent=2)+'\n')
if any(v!=0 for v in phases.values()):raise SystemExit(1)
np=Path('/data/neon-build/validation/normal-stock-preparation-native-20261101T160748Z-e5a558-native')
sp=np/'budget-spec.json'
deadline=time.monotonic()+180
while True:
 r=subprocess.run(['/usr/bin/python3','/data/neon-build/continuous/resource-budget/budget.py','reserve','--spec',str(sp)],capture_output=True,text=True)
 (p/'native-reserve.json').write_text(json.dumps({'returncode':r.returncode,'stdout':r.stdout,'stderr':r.stderr},indent=2)+'\n')
 if r.returncode==0:break
 if r.returncode!=75 or time.monotonic()>=deadline:raise SystemExit(r.returncode)
 time.sleep(5)
r=subprocess.run(['/usr/bin/python3','/data/neon-build/continuous/resource-budget/budget.py','launch','--spec',str(sp)],capture_output=True,text=True)
(p/'native-launch.json').write_text(json.dumps({'returncode':r.returncode,'stdout':r.stdout,'stderr':r.stderr},indent=2)+'\n')
raise SystemExit(r.returncode)
