from pathlib import Path
import subprocess,json,socket,time,os,hashlib,re,base64
out=Path(__file__).parent
kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(kernel/'vmlinuz')=='3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0'
assert sha(kernel/'kernel.config')=='11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be'
assert sha(kernel/'kernel-notes.bin')=='7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828'
challenge=json.loads((out/'root/fixture/challenge.json').read_text());base=int(challenge['original_base_bytes']);minimum=int(challenge['business_minimum_bytes']);delta=minimum-base
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
name=out.name.lower()
args=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m',str(base//(1<<20))+'M,slots=1,maxmem='+str(minimum//(1<<20))+'M','-object','memory-backend-ram,id=mem0,size='+str(delta),'-device','virtio-mem-pci,id=vmem0,memdev=mem0,node=0,requested-size=0,block-size='+str(int(challenge['identity']['block_bytes'])),'-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/results/qmp.sock,server=on,wait=off','-drive','file=/results/state.ext4,if=virtio,format=raw,cache=none','-drive','file=/results/runtime.iso,if=virtio,format=raw,readonly=on','-kernel','/kernel/vmlinuz','-initrd','/probe/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=256M '+(out/'required-kernel-args.txt').read_text().strip()]
cmd=['timeout','--signal=TERM','--kill-after=10s','120s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(out)+':/results:rw','-v',str(out)+':/probe:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',image,*args]
(out/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
def qmp(ops):
 fd=os.open(out,os.O_DIRECTORY|os.O_RDONLY)
 try:
  s=socket.socket(socket.AF_UNIX);s.settimeout(1)
  try:
   s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline());responses=[]
   for op in [{'execute':'qmp_capabilities'}]+ops:
    s.sendall((json.dumps(op)+'\n').encode())
    while True:
     x=json.loads(f.readline())
     if 'return'in x or 'error'in x:break
    if 'error'in x:raise ValueError(x)
    responses.append(x['return'])
   return responses[1:]
  finally:s.close()
 finally:os.close(fd)
start=time.monotonic();kvm=None;actualbase=None;sent=False;actualtarget=None;qmpfail=[]
with(out/'console.log').open('wb')as log:
 p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
 while p.poll()is None:
  try:
   if kvm is None:
    kvm,actualbase=qmp([{'execute':'query-kvm'},{'execute':'query-memory-size-summary'}])
    assert kvm=={'enabled':True,'present':True},kvm
    assert actualbase=={'base-memory':base,'plugged-memory':0},actualbase
   text=(out/'console.log').read_text(errors='replace')
   if not sent and 'INITIAL_PREBOOT_READY_FOR_HOST_QMP_GROW=1'in text:
    before=qmp([{'execute':'query-memory-size-summary'}])[0]
    assert before=={'base-memory':base,'plugged-memory':0},before
    result=qmp([{'execute':'qom-set','arguments':{'path':'/machine/peripheral/vmem0','property':'requested-size','value':delta}}])
    sent=True;(out/'grow-may-have-sent.json').write_text(json.dumps({'before':before,'original_requested_delta':delta,'qom_set_reply':result},indent=2)+'\n')
   if sent and actualtarget is None:
    summary,devices=qmp([{'execute':'query-memory-size-summary'},{'execute':'query-memory-devices'}])
    if summary=={'base-memory':base,'plugged-memory':delta}:
     actualtarget={'summary':summary,'devices':devices};(out/'actual-target-observed.json').write_text(json.dumps(actualtarget,indent=2)+'\n')
  except(OSError,ValueError,AssertionError)as e:
   if not qmpfail or qmpfail[-1]!=str(e):qmpfail.append(str(e))
  time.sleep(.025)
 rc=p.wait()
clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True)
(out/'cleanup.json').write_text(json.dumps({'rc':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr},indent=2)+'\n')
text=(out/'console.log').read_text(errors='replace');exits=re.findall(r'^INITIAL_GUEST_EXIT=(\d+)\r?$',text,re.M);notes=re.findall(r'^INITIAL_GUEST_NOTES=([^\r\n]+)',text,re.M);releases=re.findall(r'^INITIAL_GUEST_RELEASE=([^\r\n]+)',text,re.M)
records={};errors=[];(out/'reports').mkdir()
for n,h,b in re.findall(r'^INITIAL_GUEST_FILE=([^:\r\n]+):([a-f0-9]{64}):([A-Za-z0-9+/=]+)\r?$',text,re.M):
 try:
  assert n==Path(n).name and n not in records;raw=base64.b64decode(b,validate=True);assert hashlib.sha256(raw).hexdigest()==h;records[n]=json.loads(raw);(out/'reports'/n).write_bytes(raw)
 except Exception as e:errors.append(str(e))
roots=re.findall(r'^--- PASS: (TestNativeInitialPrebootGuestComponent) \(',text,re.M);subs=re.findall(r'^    --- PASS: (TestNativeInitialPrebootGuestComponent/\S+) \(',text,re.M)
passed=rc==0 and clean.returncode==0 and kvm=={'enabled':True,'present':True} and exits==['0'] and notes==['7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828'] and len(roots)==1 and len(subs)==2 and len(records)==5 and actualbase=={'base-memory':base,'plugged-memory':0} and actualtarget is not None and not errors
result={'id':'NEON-S-INITIAL-GUEST-001','status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT','host_qemu_exit':rc,'Guest_exits':exits,'actual_qmp_kvm':kvm,'actual_initial_base':actualbase,'grow_sent':sent,'actual_target':actualtarget,'qmp_errors':qmpfail,'seconds':time.monotonic()-start,'roots':roots,'subcases':subs,'reports':list(records),'errors':errors,'go_binary_sha256':sha(out/'root/initial-guest.test'),'kernel_image_sha256':sha(kernel/'vmlinuz'),'kernel_config_sha256':sha(kernel/'kernel.config'),'kernel_notes_sha256':sha(kernel/'kernel-notes.bin'),'console_sha256':sha(out/'console.log'),'Host_source_fixture':True,'PG_started':False,'normal_authorized':False,'E':0}
(out/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True);raise SystemExit(0 if passed else 1)
