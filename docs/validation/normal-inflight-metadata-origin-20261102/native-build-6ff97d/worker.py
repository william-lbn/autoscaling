from pathlib import Path
import os,json,subprocess,time,hashlib
p=Path(__file__).parent;start=time.monotonic();raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout;env=dict(os.environ);allowed={'PATH','DOCKER_CONFIG','DOCKER_HOST','DOCKER_CONTEXT','BUILDX_BUILDER','HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy','NO_PROXY','no_proxy'};env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in[x.split(b'=',1)]if k.decode()in allowed})
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=4g','--resource','memory-swap=4g','--resource','cpu-quota=400000','--network','none','--progress','plain','--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')];(p/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with(p/'run.log').open('wb')as log:rc=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
names=['cli','builder','native-compile','native-target-list','host-compile','host-export','fixture-seed'];phases={key:int((p/'export'/(key+'.exitcode')).read_text())if(p/'export'/(key+'.exitcode')).exists()else None for key in names};result={'build_exit':rc,'phases':phases,'seconds':time.monotonic()-start,'inherited_U_not_repeated':True,'Source':False,'normal':False,'E':0};(p/'result.json').write_text(json.dumps(result,indent=2)+'\n')
if rc or any(x!=0 for x in phases.values()):raise SystemExit(rc or 1)
assert(p/'export/native-target-list.txt').read_text().splitlines()==['TestNativeNormalStockUdevPreparationInitial']
(p/'artifacts.json').write_text(json.dumps({name:hashlib.sha256((p/'export'/name).read_bytes()).hexdigest()for name in['service.test','host-memory.test','resource-guard.real','vm-builder.real']},indent=2)+'\n')
np=Path('/data/neon-build/validation/normal-inflight-metadata-origin-native-20261101T203708Z-6ff97d-native')
cli='/data/neon-build/continuous/resource-budget/budget.py';sp=np/'budget-spec.json';deadline=time.monotonic()+180
for action in['reserve','launch']:
 while True:
  x=subprocess.run(['python3',cli,action,'--spec',str(sp)],capture_output=True,text=True);(p/('native-'+action+'.json')).write_text(json.dumps({'exit':x.returncode,'stdout':x.stdout,'stderr':x.stderr},indent=2)+'\n')
  if x.returncode==0:break
  if x.returncode!=75 or time.monotonic()>=deadline:raise SystemExit(x.returncode)
  time.sleep(3)
