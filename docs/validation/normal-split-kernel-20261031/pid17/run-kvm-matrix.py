import subprocess,json,hashlib,datetime,time,concurrent.futures,re,socket,os
from pathlib import Path
out=Path("/data/neon-build/validation/normal-split-pid17-20261030T225434Z");root=out/'runtime'
build=json.loads((out/'build-result.json').read_text());assert build['exitCode']==0
kernel=out/'export/vmlinuz';sha=hashlib.sha256(kernel.read_bytes()).hexdigest();assert sha==build['vmlinuzSha256']
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
initrd=root/'initramfs.cpio.gz';initsha=hashlib.sha256(initrd.read_bytes()).hexdigest()
jobs=[
 ('cap512','neonvm.pid_max_limit=512 pg17_pid_expected=512 pg17_pid_capabilities=1',False,None),
 ('late-sysctl','neonvm.pid_max_limit=512 kernel.pid_max=4096 pg17_pid_expected=512',False,None),
 ('default-control','pg17_pid_expected=0',False,None),
 ('boundary301','neonvm.pid_max_limit=301 pg17_pid_expected=301',False,None),
 ('boundary4096','neonvm.pid_max_limit=4096 pg17_pid_expected=4096',False,None),
 ('empty','neonvm.pid_max_limit=',True,'canonical decimal value required'),
 ('bare','neonvm.pid_max_limit',True,'canonical decimal value required'),
 ('leading-zero','neonvm.pid_max_limit=0512',True,'canonical decimal value required'),
 ('zero','neonvm.pid_max_limit=0',True,'canonical decimal value required'),
 ('plus','neonvm.pid_max_limit=+512',True,'canonical decimal value required'),
 ('negative','neonvm.pid_max_limit=-512',True,'canonical decimal value required'),
 ('nondecimal','neonvm.pid_max_limit=512x',True,'canonical decimal value required'),
 ('overflow','neonvm.pid_max_limit=9999999999999999999999999999999',True,'value outside supported range'),
 ('below301','neonvm.pid_max_limit=300',True,'value outside supported range'),
 ('above4096','neonvm.pid_max_limit=4097',True,'value outside supported range'),
 ('duplicate','neonvm.pid_max_limit=512 neonvm.pid_max_limit=512',True,'duplicate boot policy'),
 ('duplicate-different','neonvm.pid_max_limit=512 neonvm.pid_max_limit=301',True,'duplicate boot policy')]

def query_actual_kvm(r):
 fd=os.open(r,os.O_RDONLY|os.O_DIRECTORY)
 try:
  s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);s.settimeout(.5)
  try:
   s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline())
   for cmd in ['qmp_capabilities','query-kvm']:
    s.sendall((json.dumps({'execute':cmd})+'\n').encode())
    while True:
     data=json.loads(f.readline())
     if 'return' in data or 'error' in data:break
    if 'error' in data:raise RuntimeError(data)
   result=data['return'];assert result['enabled'] is True and result['present'] is True
   (r/'actual-qmp-kvm.json').write_text(json.dumps(result,indent=2)+'\n');return result
  finally:s.close()
 finally:os.close(fd)

def run(job):
 label,policy,negative,reason=job;r=root/label;r.mkdir()
 name='pg17-pidcap-'+label+'-normal-split-pid17-20261030t225434z'
 cmdline='console=ttyS0,115200 earlycon=uart8250,io,0x3f8,115200n8 rdinit=/init panic=-1 kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf '+policy
 qemu=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','1024M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/results/qmp.sock,server=on,wait=off','-kernel','/kernel/vmlinuz','-initrd','/probe/initramfs.cpio.gz','-append',cmdline]
 docker=['docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--ulimit','core=0','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(r)+':/results:rw','-v',str(root)+':/probe:ro','-v',str(kernel.parent)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',image,*qemu]
 cmd=['timeout','--signal=TERM','--kill-after=10s','120s',*docker]
 (r/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
 tick=time.monotonic();start=datetime.datetime.now(datetime.timezone.utc).isoformat()
 intentional=False;stop=None;last='';actual_qmp=None
 try:
  with (r/'console.log').open('wb') as log:
   proc=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
   while proc.poll() is None:
    time.sleep(.05)
    text=(r/'console.log').read_text(errors='replace')
    if actual_qmp is None:
     try:actual_qmp=query_actual_kvm(r)
     except (OSError,ValueError):pass
    if negative and re.search(r'Kernel panic - not syncing: neonvm\.pid_max_limit: [^\r\n]+',text):
     intentional=True
     ins=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
     if ins.returncode==0:
      i=json.loads(ins.stdout)[0];(r/'pre-stop-state.json').write_text(json.dumps({'Id':i['Id'],'Image':i['Image'],'State':i['State'],'NetworkMode':i['HostConfig']['NetworkMode']},indent=2)+'\n')
     stop=subprocess.run(['docker','kill','--signal','TERM',name],capture_output=True,text=True)
     break
   rc=proc.wait(timeout=15)
 finally:
  ins=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
  if ins.returncode==0:
   i=json.loads(ins.stdout)[0];(r/'container-state.json').write_text(json.dumps({'Id':i['Id'],'Image':i['Image'],'State':i['State'],'NetworkMode':i['HostConfig']['NetworkMode']},indent=2)+'\n')
  clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True)
  (r/'cleanup.json').write_text(json.dumps({'exitCode':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr},indent=2)+'\n')
 text=(r/'console.log').read_text(errors='replace')
 panic=re.findall(r'^\[\s*[0-9.]+\] Kernel panic - not syncing: ([^\r\n]+)\r?$',text,re.M)
 probe=[json.loads(l.split('=',1)[1]) for l in text.splitlines() if l.startswith('PG17_PID_PROBE={')]
 result={'case':label,'actualQMPKVM':actual_qmp,'negativeBoot':negative,'hostQemuExitCode':rc,'elapsedSeconds':time.monotonic()-tick,'startUTC':start,'endUTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'kernelSha256':sha,'initramfsSha256':initsha,'QEMUImage':image,'accel':'kvm','TCGFallback':False,'GuestNIC':'none','consoleSha256':hashlib.sha256((r/'console.log').read_bytes()).hexdigest(),'panicMessages':panic,'intentionallyTerminatedAfterObservedPanic':intentional,'guestInitMarkerPresent':'PG17_PID_INIT_BEGIN' in text,'kernelRunInitMarkerPresent':'Run /init as init process' in text,'probe':probe}
 if stop is not None:result['panicStopCommand']={'exitCode':stop.returncode,'stdout':stop.stdout,'stderr':stop.stderr}
 if negative:
  result['pass']=actual_qmp is not None and intentional and panic==['neonvm.pid_max_limit: '+reason] and not result['guestInitMarkerPresent'] and not result['kernelRunInitMarkerPresent'] and not probe
 else:
  result['pass']=actual_qmp is not None and rc==0 and 'PG17_PID_GUEST_EXIT=0' in text and 'PG17_DIRECT_USER_EXEC=success' in text and 'PG17_PID_PROBE_EXIT=0' in text and len(probe)==1 and probe[0]['pass'] and not panic and 'PG17_PID_RELEASE='+build['actualBuildRelease'] in text and 'PG17_PID_NOTES_SHA256='+build['buildNotesSha256'] in text
  if label=='cap512':
   result['pass']=result['pass'] and 'PG17_PID_UMH_MARKER=absent' in text and 'PG17_PID_UMH_TRIGGER_EXIT=0' in text and 'PG17_PID_LANDLOCK_EXIT=0' in text and '"abi":6' in text and '"enforced":true' in text and '"valid_nested_namespace":true' in text and '"exclusive_cap_errno":22' in text
 (r/'run.json').write_text(json.dumps(result,indent=2)+'\n')
 print(json.dumps(result),flush=True)
 if not result['pass']:print('\n'.join(text.splitlines()[-45:]),flush=True)
 return result
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(run,jobs))
(root/'matrix-result.json').write_text(json.dumps({'id':'PG17-S-PIDCAP-002','scope':'real isolated KVM kernel component only','results':results,'pass':all(x['pass'] for x in results),'vmShrinkPermit':False,'PGStarted':False,'normalDecreaseExecuted':False,'KubernetesDeployed':False},indent=2)+'\n')
raise SystemExit(0 if all(x['pass'] for x in results) else 1)
