from pathlib import Path
import subprocess,os,time,json,hashlib,traceback
p=Path(__file__).parent;start=time.monotonic();lock=json.loads((p/'source-lock.json').read_text());result={'Source':False,'normal':False,'E':0};name='system-role-closure-component-'+p.name[-21:].lower()
raw=subprocess.check_output(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0']);env=os.environ.copy();env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in[x.split(b'=',1)]})
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=4g','--resource','memory-swap=4g','--resource','cpu-quota=400000','--network','none','--progress','plain','--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
try:
 with(p/'run.log').open('wb')as log:result['build_exit']=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=420).returncode
 phases=['u0','u1','cli','darwin','component-compile']
 for n in phases:
  f=p/'export'/(n+'.exitcode');result[n+'_exit']=int(f.read_text())if f.exists()else None
 events=[]
 for n in ['u0','u1']:
  f=p/'export'/(n+'.json')
  if f.exists():events +=[json.loads(l)for l in f.read_text().splitlines()if l.strip()]
 passed=[e['Test']for e in events if e.get('Action')=='pass'and e.get('Test')];result['unit_passed_roots']=sorted(t for t in passed if'/'not in t);result['unit_subtests']=sum('/'in t for t in passed);expected=sum(lock['unit_targets'].values(),[]);result['missing_units']=sorted(set(expected)-set(passed));result['skipped_units']=[e.get('Test')for e in events if e.get('Action')=='skip']
 if result['build_exit']==0 and all(result[n+'_exit']==0 for n in phases)and not result['missing_units']and not result['skipped_units']:
  (p/'component-evidence').mkdir();component=['docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--user','0:0','--cpus','2','--memory','3g','--memory-swap','3g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,size=256m','-v',str(p/'export')+':/report:ro','-v',lock['provider_root']+':/original-rootfs:ro','-v',str(p/'component-evidence')+':/evidence:rw','-v',lock['oracle_root']+':/complete-oracle:ro','-e','NEON_SYSTEM_ORIGINAL_ROOTFS=/original-rootfs','-e','NEON_SYSTEM_COMPLETE_ORACLE_ROOTFS=/complete-oracle','--entrypoint','/bin/bash','127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1','-c','set +e; /opt/go/bin/go tool test2json -p vm-builder -t /report/native-builder.test -test.v -test.run ^TestNormalSystemBuildOriginal105To119And40ELFs$; rc=$?; for metric in cpu.stat memory.peak io.stat memory.events; do cat /sys/fs/cgroup/$metric >/evidence/$metric; done; exit "$rc"']
  (p/'component-command.json').write_text(json.dumps(component,indent=2)+'\n')
  with(p/'component.json').open('w')as log:result['component_exit']=subprocess.run(component,stdout=log,stderr=subprocess.STDOUT,timeout=90).returncode
  x=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
  if x.returncode==0:
   o=json.loads(x.stdout)[0];(p/'component-container.json').write_text(json.dumps({'ID':o['Id'],'Image':o['Image'],'State':o['State'],'HostConfig':{k:o['HostConfig'].get(k)for k in ['Memory','MemorySwap','NanoCpus','ReadonlyRootfs','NetworkMode','PidsLimit','SecurityOpt','CapDrop']}},indent=2)+'\n');assert not o['State']['OOMKilled']
  ev=[json.loads(l)for l in(p/'component.json').read_text().splitlines()if l.strip()];result['component_passed']=[e['Test']for e in ev if e.get('Action')=='pass'and e.get('Test')];result['component_skipped']=[e.get('Test')for e in ev if e.get('Action')=='skip']
  provider=Path(lock['provider_root']);result['provider_after_sha256']={n:hashlib.sha256((provider/n).read_bytes()).hexdigest()for n in lock['provider_before_sha256']};assert result['provider_after_sha256']==lock['provider_before_sha256'];result['oracle_after_sha256']=hashlib.sha256((Path(lock['oracle_root'])/'neonvm/config/dsga-normal-system-source.json').read_bytes()).hexdigest();assert result['oracle_after_sha256']==lock['oracle_sha256']
except BaseException:
 result['driver_error']=True;(p/'driver-error.txt').write_text(traceback.format_exc())
finally:
 x=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(p/'cleanup.json').write_text(json.dumps({'exit':x.returncode,'stdout':x.stdout,'stderr':x.stderr},indent=2)+'\n')
result['seconds']=time.monotonic()-start;(p/'result.json').write_text(json.dumps(result,indent=2)+'\n');raise SystemExit(0 if result.get('build_exit')==0 and all(result.get(n+'_exit')==0 for n in ['u0','u1','cli','darwin','component-compile'])and not result.get('missing_units')and not result.get('skipped_units')and result.get('component_exit')==0 and result.get('component_passed')==[lock['actual_rootfs_target']]and not result.get('component_skipped')and not result.get('driver_error')else 1)
