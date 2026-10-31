from pathlib import Path
import json,subprocess,time,shlex,sys,traceback
cfg=json.loads(Path(sys.argv[1]).read_text());out=Path(cfg['out']);dev=Path(cfg['dev']);snap=Path(cfg['snapshot'])
cmd=['docker','buildx','build','--builder','neon-data','--platform','linux/amd64','--network','none','--resource','memory=4g','--resource','memory-swap=4g','--resource','cpu-quota=200000','--progress','plain','--build-arg','TEST_NONCE='+cfg['nonce'],'--metadata-file',str(out/'build-metadata.json'),'--output','type=local,dest='+str(out/'export'),'-f',str(dev/'Dockerfile'),str(snap)]
full=['bash','-lc','source /data/neon-build/config/env.sh\n'+shlex.join(cmd)]
(out/'command.json').write_text(json.dumps({'command':full,'scope':'component U only; source/policies/Host API are typed fakes; real disk/pidfd in sandbox','maximumRUNMemoryBytes':4<<30,'maximumRUNSwapBytes':0,'VMShrinkPermit':False},indent=2)+'\n')
start=time.time();result={'scope':'component U only; no Guest/QEMU/kernel/normal/E','VMShrinkPermit':False}
try:
 with (out/'build.log').open('w') as f:p=subprocess.run(full,stdout=f,stderr=subprocess.STDOUT,timeout=900)
 result['buildExitCode']=p.returncode
 if p.returncode==0:
  result['testExitCode']=int((out/'export/test.rc').read_text().strip());result['status']='PASS' if result['testExitCode']==0 else 'FAIL_TEST'
 else:result['status']='FAIL_BUILD_ENV_OR_COMPILE'
except Exception:
 result['status']='FAIL_HARNESS_OR_ENV';result['traceback']=traceback.format_exc()
result['seconds']=time.time()-start
(out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(0 if result['status']=='PASS' else 1)
