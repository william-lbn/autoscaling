from pathlib import Path
import datetime,hashlib,json,os,socket,subprocess,time
out=Path(__file__).parent;inputs=json.loads((out/'inputs.json').read_text());cases=json.loads((out/'cases.json').read_text());kernel=Path(inputs['kernel_directory']);results=[]
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
def qmp(case):
 fd=os.open(case,os.O_DIRECTORY|os.O_RDONLY)
 try:
  sock=socket.socket(socket.AF_UNIX);sock.settimeout(.5)
  try:
   sock.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=sock.makefile('rb');frames=[json.loads(f.readline())]
   for seq,op in enumerate(['qmp_capabilities','query-kvm','query-version'],1):
    sock.sendall((json.dumps({'execute':op,'id':seq})+'\n').encode())
    while True:
     x=json.loads(f.readline());frames.append(x)
     if x.get('id')==seq:break
    assert 'error'not in x,x
    if op=='query-kvm':kvm=x['return'];assert kvm['enabled']and kvm['present']
   return kvm,frames
  finally:sock.close()
 finally:os.close(fd)
for item in cases:
 mode=item['mode'];case=out/mode;name='neon-userns-v2-'+inputs['nonce'].lower()+'-'+mode
 qemu=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m',str(item['memory_mib'])+'M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/results/qmp.sock,server=on,wait=off','-drive','file=/results/state.ext4,if=virtio,format=raw,cache=none','-drive','file=/probe/runtime.iso,if=virtio,format=raw,readonly=on','-kernel','/kernel/vmlinuz','-initrd','/case/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=512M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512 neonvm.dsga.kernel_bootstrap=normal-split-metadata-v2']
 cmd=['timeout','--signal=TERM','--kill-after=10s','150s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(case)+':/results:rw','-v',str(case)+':/case:ro','-v',str(out)+':/probe:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',image,*qemu]
 (case/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');start=time.monotonic();kvm=None;frames=[];error=None
 with(case/'console.log').open('wb')as log:
  p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
  while p.poll()is None:
   if kvm is None:
    try:kvm,frames=qmp(case)
    except(OSError,ValueError,AssertionError)as e:error=repr(e)
   time.sleep(.05)
  rc=p.wait()
 (case/'qmp-frames.json').write_text(json.dumps(frames,indent=2)+'\n')
 x=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if x.returncode==0:
  obj=json.loads(x.stdout)[0];(case/'container-state.json').write_text(json.dumps({'Id':obj['Id'],'Image':obj['Image'],'State':obj['State']},indent=2)+'\n')
 x=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(case/'cleanup.json').write_text(json.dumps({'exit_code':x.returncode,'stdout':x.stdout,'stderr':x.stderr},indent=2)+'\n')
 text=(case/'console.log').read_text(errors='replace');records=[json.loads(l.split('=',1)[1])for l in text.splitlines()if l.startswith('V2_USERNS_COMPONENT={')]
 passed=rc==0 and kvm is not None and len(records)==1 and text.count('KERNEL_PREP_V2_GUEST_EXIT=0')==1 and '--- PASS: TestNativeKernelPrepUserNSV2Component'in text and 'V2_USERNS_BEFORE_POSITIVE'in text and 'V2_USERNS_AFTER_NEGATIVE'in text and 'Kernel panic - not syncing:'not in text and 'BUG:'not in text and 'WARNING:'not in text and 'V2_KERNEL_NOTES='+inputs['kernel_notes_sha256']in text
 result={'mode':mode,'status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT','host_QEMU_exit':rc,'actual_QMP_KVM':kvm,'last_QMP_retry_error':error,'seconds':time.monotonic()-start,'record':records,'console_sha256':hashlib.sha256((case/'console.log').read_bytes()).hexdigest(),'PG_started':False,'all_sources_closed':False,'normal_authorized':False,'E':0}
 (case/'result.json').write_text(json.dumps(result,indent=2)+'\n');(case/'exitcode').write_text(('0'if passed else'1')+'\n');print(json.dumps(result),flush=True);results.append(result)
summary={'status':'PASS_COMPONENT_ONLY'if len(results)==2 and all(x['status']=='PASS_COMPONENT_ONLY'for x in results)else'FAIL_COMPONENT','cases':len(results),'passed':sum(x['status']=='PASS_COMPONENT_ONLY'for x in results),'results':results,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'normal_authorized':False,'E':0};(out/'result.json').write_text(json.dumps(summary,indent=2)+'\n');(out/'exitcode').write_text(('0'if summary['status']=='PASS_COMPONENT_ONLY'else'1')+'\n');raise SystemExit(int(summary['status']!='PASS_COMPONENT_ONLY'))
