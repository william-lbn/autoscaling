from pathlib import Path
import subprocess,json,socket,time,os,hashlib,re,base64
out=Path('/data/neon-build/validation/normal-system-v2-native-20261031T192132Z-bb29ab');kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export');assert hashlib.sha256((kernel/'vmlinuz').read_bytes()).hexdigest()=='3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0'
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e';name=out.name.lower()
args=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','1024M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/results/qmp.sock,server=on,wait=off','-kernel','/kernel/vmlinuz','-initrd','/probe/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512 dsga_system_source=1']
cmd=['timeout','--signal=TERM','--kill-after=10s','120s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(out)+':/results:rw','-v',str(out)+':/probe:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',image,*args];(out/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
def qmp():
 fd=os.open(out,os.O_DIRECTORY|os.O_RDONLY)
 try:
  s=socket.socket(socket.AF_UNIX);s.settimeout(.5)
  try:
   s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline());result=[]
   for op in ['qmp_capabilities','query-kvm','query-version']:
    s.sendall((json.dumps({'execute':op})+'\n').encode())
    while True:
     x=json.loads(f.readline())
     if 'return'in x or'error'in x:break
    assert'error'not in x,x;result.append({'command':op,'response':x})
   assert result[1]['response']['return']=={'enabled':True,'present':True};return result
  finally:s.close()
 finally:os.close(fd)
start=time.monotonic();responses=None
with(out/'console.log').open('wb')as log:
 p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
 while p.poll()is None:
  if responses is None:
   try:responses=qmp()
   except(OSError,ValueError):pass
  time.sleep(.05)
 rc=p.wait()
clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(out/'cleanup.json').write_text(json.dumps({'rc':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr},indent=2)+'\n');text=(out/'console.log').read_text(errors='replace');notes=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text);raw=base64.b64decode(''.join(notes[1].split()),validate=True)if notes else None
records=[json.loads(l.split('=',1)[1])for l in text.splitlines()if l.startswith('DSGA_NATIVE_SYSTEM_SOURCE={')]
passed=rc==0 and responses is not None and'SYSTEM_SOURCE_GUEST_EXIT=0'in text and raw==(kernel/'kernel-notes.bin').read_bytes()and'Kernel panic - not syncing:'not in text and len(records)==1 and'--- PASS: TestNativeNormalSystemV2Component'in text
result={'id':'NEON-S-SYSTEM-SOURCE-V2-001','observations':records,'binary_sha256':hashlib.sha256((out/'initramfs-root/system-source.test').read_bytes()).hexdigest(),'status':'PASS_FIXTURE_COMPONENT_ONLY'if passed else'FAIL_COMPONENT','host_qemu_exit':rc,'elapsed_seconds':time.monotonic()-start,'actual_qmp':responses,'runtime_kernel_notes_sha256':hashlib.sha256(raw).hexdigest()if raw else None,'console_sha256':hashlib.sha256((out/'console.log').read_bytes()).hexdigest(),'normal_permit':False,'all_sources_closed':False,'actual_services_or_PG_started':False};(out/'result.json').write_text(json.dumps(result,indent=2)+'\n');(out/'exitcode').write_text(('0'if passed else'1')+'\n');print(json.dumps(result),flush=True);raise SystemExit(int(not passed))
