from pathlib import Path
import json,subprocess,time,re,base64,hashlib,traceback
out=Path('/data/neon-build/validation/initial-lifecycle-native-20261031T181834Z')
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
name='initial-life-kvm-20261031t181834z-a01'
kernel='/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export'
cmd=['timeout','--signal=TERM','--kill-after=10s','140s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--cap-add','SYS_PTRACE','--cap-add','KILL','--cap-add','SETUID','--cap-add','SETGID','--cap-add','DAC_OVERRIDE','--user','0:0','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,size=256m,mode=1777','-e','GOMAXPROCS=2','-e','DSGA_INITIAL_HOST_NATIVE=qemu369-v1','-e','DSGA_INITIAL_HOST_OUTPUT=/proof','-v',str(out)+':/proof:rw','-v',str(out/'export/host.test')+':/native-host.test:ro','-v',kernel+':/kernel:ro','--entrypoint','/native-host.test',image,'-test.run=^TestNativeInitialLifecycleComponent$','-test.v','-test.count=1','-test.timeout=120s']
(out/'kvm-command.json').write_text(json.dumps({'command':cmd,'HostNoNetworkOrCgroupMutation':True,'KVMRequiredNoTCG':True,'scope':'actual initial lifecycle component; Node/API budget fake, not normal/E'},indent=2)+'\n')
result={'id':'PG17-S-INITIAL-LIFECYCLE-001','status':'FAIL_COMPONENT','HostAPIIsModel':True,'formalBootstrapStarted':False,'PGStarted':False,'normalAuthorized':False,'E':0}
start=time.monotonic()
try:
 with(out/'host-native.log').open('w')as log:
  r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,timeout=155)
 result['HostContainerExit']=r.returncode
 x=subprocess.run(['docker','inspect',name],capture_output=True,text=True);(out/'kvm-container-inspect.json').write_text(x.stdout if x.returncode==0 else json.dumps({'exit':x.returncode,'stderr':x.stderr}))
 text=(out/'console.log').read_text(errors='replace')if(out/'console.log').exists()else''
 result['GuestExitMarkers']=re.findall(r'^INITIAL_FIXED_GUEST_EXIT=(\d+)\r?$',text,re.M)
 wal={};(out/'Guest-WAL').mkdir(exist_ok=True)
 for n,h,b in re.findall(r'^INITIAL_FIXED_GUEST_WAL=([^:\r\n]+):([a-f0-9]{64}):([A-Za-z0-9+/=]+)\r?$',text,re.M):
  assert n==Path(n).name and n not in wal;raw=base64.b64decode(b,validate=True);assert hashlib.sha256(raw).hexdigest()==h;wal[n]=h;(out/'Guest-WAL'/n).write_bytes(raw)
 result['actualGuestWALCount']=len(wal)
 physical=re.findall(r'^INITIAL_FIXED_GUEST_PHYSICAL=([a-f0-9]{64}):([A-Za-z0-9+/=]+)\r?$',text,re.M)
 if len(physical)==1:
  h,b=physical[0];raw=base64.b64decode(b,validate=True);assert hashlib.sha256(raw).hexdigest()==h;(out/'actual-Guest-physical.json').write_bytes(raw)
 result['actualGuestPhysicalCount']=len(physical)
 result['postBootstrapExplicitlyNotRun']='INITIAL_FIXED_GUEST_POST_BOOTSTRAP_NOT_RUN=1'in text
 if r.returncode==0 and result['GuestExitMarkers']==['0'] and len(wal)==8 and len(physical)==1 and(out/'terminal.json').exists():
  proof=[]
  for n,h in wal.items():
   x=subprocess.run(['debugfs','-R','cat /state/'+n,str(out/'state.ext4')],capture_output=True)
   assert x.returncode==0 and hashlib.sha256(x.stdout).hexdigest()==h,(n,x.returncode)
   proof.append({'name':n,'sha256':h,'debugfsExit':x.returncode})
  (out/'Guest-offline-ext4-proof.json').write_text(json.dumps({'source':'original stopped QEMU file disk; readonly debugfs; no mount','files':proof},indent=2)+'\n')
  result['status']='PASS_COMPONENT_ONLY'
except Exception:
 result['exception']=traceback.format_exc()
finally:
 r=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True)
 (out/'kvm-cleanup.json').write_text(json.dumps({'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr},indent=2)+'\n')
 result['seconds']=time.monotonic()-start
 (out/'kvm-result.json').write_text(json.dumps(result,indent=2)+'\n')
 print(json.dumps(result),flush=True)
raise SystemExit(0 if result['status']=='PASS_COMPONENT_ONLY'else 1)
