from pathlib import Path
import subprocess,json,socket,time,os,hashlib
out=Path('/data/neon-build/validation/presys-v2-native-20261031T005721Z');cases=json.loads((out/'cases.json').read_text());results=[];kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export');assert hashlib.sha256((kernel/'vmlinuz').read_bytes()).hexdigest()=='3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0'
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
def qmp(case):
 fd=os.open(case,os.O_DIRECTORY|os.O_RDONLY)
 try:
  s=socket.socket(socket.AF_UNIX);s.settimeout(.5)
  try:
   s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline())
   for op in ['qmp_capabilities','query-kvm']:
    s.sendall((json.dumps({'execute':op})+'\n').encode())
    while True:
     x=json.loads(f.readline())
     if 'return'in x or 'error'in x:break
    assert'error'not in x,x
   assert x['return']['enabled']and x['return']['present'];return x['return']
  finally:s.close()
 finally:os.close(fd)
for item in cases:
 mode=item['mode'];case=out/mode;name=out.name.lower()+'-'+mode
 qemu=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m',str(item['memory_mib'])+'M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/results/qmp.sock,server=on,wait=off','-drive','file=/results/state.ext4,if=virtio,format=raw,cache=none','-drive','file=/probe/runtime.iso,if=virtio,format=raw,readonly=on','-kernel','/kernel/vmlinuz','-initrd','/case/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512 neonvm.dsga.kernel_bootstrap=normal-split-v1 neonvm.dsga.physical_profile=kernel-v1 neonvm.dsga.prebootstrap_system=normal-split-v1']
 cmd=['timeout','--signal=TERM','--kill-after=10s','150s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(case)+':/results:rw','-v',str(case)+':/case:ro','-v',str(out)+':/probe:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',image,*qemu]
 (case/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');start=time.monotonic();kvm=None
 with(case/'console.log').open('wb')as log:
  p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
  while p.poll()is None:
   if kvm is None:
    try:kvm=qmp(case)
    except(OSError,ValueError):pass
   time.sleep(.05)
  rc=p.wait()
 x=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if x.returncode==0:
  v=json.loads(x.stdout)[0];(case/'container-state.json').write_text(json.dumps({'Id':v['Id'],'Image':v['Image'],'State':v['State']},indent=2)+'\n')
 x=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(case/'cleanup.json').write_text(json.dumps({'exit_code':x.returncode,'stdout':x.stdout,'stderr':x.stderr},indent=2)+'\n')
 text=(case/'console.log').read_text(errors='replace');records=[json.loads(l.split('=',1)[1])for l in text.splitlines()if l.startswith(('PRESYS_NATIVE_COMPONENT={','PRESYS_NATIVE_NEGATIVE={'))];passed=rc==0 and kvm is not None and len(records)==1 and 'PRESYS_NATIVE_GUEST_EXIT=0'in text and '--- PASS: TestNativePreSystemSplitComponent'in text and 'Kernel panic - not syncing:'not in text and 'BUG:'not in text and 'WARNING:'not in text
 result={'mode':mode,'status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT','host_qemu_exit':rc,'actual_qmp_kvm':kvm,'elapsed_seconds':time.monotonic()-start,'record':records,'console_sha256':hashlib.sha256((case/'console.log').read_bytes()).hexdigest(),'binary_sha256':'f1db711b0eb5beab573c74f7fc549d1ff0d71cf982dc19145b0730e5d0db0e43','all_sources_closed':False,'vm_shrink_permit':False,'PG_started':False,'E2E':False}
 (case/'result.json').write_text(json.dumps(result,indent=2)+'\n');(case/'exitcode').write_text(('0'if passed else'1')+'\n');print(json.dumps(result),flush=True);results.append(result)
summary={'status':'PASS_COMPONENT_ONLY'if all(x['status']=='PASS_COMPONENT_ONLY'for x in results)else'FAIL_COMPONENT','cases':len(results),'passed':sum(x['status']=='PASS_COMPONENT_ONLY'for x in results),'results':results};(out/'result.json').write_text(json.dumps(summary,indent=2)+'\n');(out/'exitcode').write_text(('0'if summary['status']=='PASS_COMPONENT_ONLY'else'1')+'\n');raise SystemExit(int(summary['status']!='PASS_COMPONENT_ONLY'))
