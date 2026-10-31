from pathlib import Path
import subprocess,json,socket,time,os,hashlib,datetime
out=Path('/data/neon-build/validation/physical-reviewed-reader-20261030T232919Z');cases=json.loads((out/'cases.json').read_text());results=[]
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
     v=json.loads(f.readline())
     if 'return' in v or 'error' in v:break
    assert 'error' not in v,v
   assert v['return']['enabled'] and v['return']['present'];return v['return']
  finally:s.close()
 finally:os.close(fd)
for data in cases:
 case=out/data['name'];name=out.name.lower()+'-'+data['name'];kernel=Path(data['kernel_path'])
 cmdline='console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512 dsga_reviewed_kernel_reader=1'
 qemu=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','1024M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/results/qmp.sock,server=on,wait=off','-kernel','/kernel/vmlinuz','-initrd','/probe/initramfs.cpio.gz','-append',cmdline]
 cmd=['timeout','--signal=TERM','--kill-after=10s','150s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(case)+':/results:rw','-v',str(case)+':/probe:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',image,*qemu]
 (case/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');start=time.monotonic();kvm=None
 with (case/'console.log').open('wb') as log:
  p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
  while p.poll() is None:
   if kvm is None:
    try:kvm=qmp(case)
    except (OSError,ValueError):pass
   time.sleep(.05)
  rc=p.wait()
 x=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if x.returncode==0:
  i=json.loads(x.stdout)[0];(case/'container-state.json').write_text(json.dumps({'Id':i['Id'],'Image':i['Image'],'State':i['State'],'NetworkMode':i['HostConfig']['NetworkMode']},indent=2)+'\n')
 x=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(case/'cleanup.json').write_text(json.dumps({'exit_code':x.returncode,'stdout':x.stdout,'stderr':x.stderr},indent=2)+'\n')
 console=(case/'console.log').read_text(errors='replace');records=[json.loads(l.split('=',1)[1]) for l in console.splitlines() if l.startswith('DSGA_NATIVE_REVIEWED_KERNEL_COMPONENT={')];passed=rc==0 and kvm is not None and len(records)==1 and 'DSGA_REVIEWED_READER_GUEST_EXIT=0' in console and '--- PASS: TestNativeReviewedPhysicalKernelComponent' in console and 'Kernel panic - not syncing:' not in console and 'WARNING:' not in console and 'BUG:' not in console
 if records:
  o=records[0]['observation'];passed=passed and o['kernel_notes_sha256']==data['notes_sha256'] and o['runtime_capabilities']['hard_pid_max_limit']==512 and not o['vm_shrink_permit']
 result={'id':data['id'],'status':'PASS_COMPONENT_ONLY' if passed else 'FAIL_COMPONENT','host_qemu_exit':rc,'actual_qmp_kvm':kvm,'elapsed_seconds':time.monotonic()-start,'kernel':data,'binary_sha256':'1f892b5caaa6c95c291f34bd5d66fb033e44b21bf884703d1b32fd8eeeafc8b7','console_sha256':hashlib.sha256((case/'console.log').read_bytes()).hexdigest(),'receipt':records,'PG_started':False,'Root_origin_created':False,'all_sources_closed':False,'vm_shrink_permit':False}
 (case/'result.json').write_text(json.dumps(result,indent=2)+'\n');(case/'exitcode').write_text(('0' if passed else '1')+'\n');print(json.dumps(result),flush=True);results.append(result)
summary={'status':'PASS_COMPONENT_ONLY' if all(x['status']=='PASS_COMPONENT_ONLY' for x in results) else 'FAIL_COMPONENT','cases':len(results),'passed':sum(x['status']=='PASS_COMPONENT_ONLY' for x in results),'results':results};(out/'result.json').write_text(json.dumps(summary,indent=2)+'\n');(out/'exitcode').write_text(('0' if summary['status']=='PASS_COMPONENT_ONLY' else '1')+'\n');raise SystemExit(int(summary['status']!='PASS_COMPONENT_ONLY'))
