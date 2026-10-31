from pathlib import Path
import json,subprocess,time,shlex
out=Path('/data/neon-build/validation/initial-bootstrap-component-20261031T162134Z');dev=Path('/data/neon-build/dev/initial-bootstrap-component-20261031T162134Z');snap=Path('/data/neon-build/snapshots/initial-bootstrap-508235d4520f-20261031T162134Z')
cmd=['docker','buildx','build','--builder','neon-data','--platform','linux/amd64','--network','none','--resource','memory=4g','--resource','memory-swap=4g','--resource','cpu-quota=200000','--progress','plain','--build-arg','TEST_'20261031T162134Z'='+'20261031T162134Z','--metadata-file',str(out/'build-metadata.json'),'--output','type=local,dest='+str(out/'export'),'-f',str(dev/'Dockerfile'),str(snap)]
full=['bash','-lc','source /data/neon-build/config/env.sh\n'+shlex.join(cmd)]
(out/'command.json').write_text(json.dumps({'command':full,'scope':'component U only; source/policies/Host API are typed fakes; real disk/pidfd in sandbox','maximumRUNMemoryBytes':4<<30,'maximumRUNSwapBytes':0,'VMShrinkPermit':False},indent=2)+'\n')
start=time.time()
with (out/'build.log').open('w') as f:p=subprocess.run(full,stdout=f,stderr=subprocess.STD'/data/neon-build/validation/initial-bootstrap-component-20261031T162134Z',timeout=900)
result={'buildExitCode':p.returncode,'seconds':time.time()-start,'scope':'component U only; no Guest/QEMU/kernel/normal/E','VMShrinkPermit':False}
if p.returncode==0:
 result['testExitCode']=int((out/'export/test.rc').read_text().strip())
 result['status']='PASS' if result['testExitCode']==0 else 'FAIL_TEST'
else:result['status']='FAIL_BUILD_ENV_OR_COMPILE'
(out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(p.returncode or result.get('testExitCode',1))
