from pathlib import Path
import subprocess,json,socket,time,os,hashlib,re,base64
out=Path('/data/neon-build/validation/array-helper-durability-native-20261031T162828Z-1e8412');kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export');assert hashlib.sha256((kernel/'vmlinuz').read_bytes()).hexdigest()=='3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0'
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e';name=out.name.lower()
args=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','1024M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/results/qmp.sock,server=on,wait=off','-drive','file=/results/state.ext4,if=virtio,format=raw,cache=none','-kernel','/kernel/vmlinuz','-initrd','/probe/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512 neon_array_populate_component=1']
cmd=['timeout','--signal=TERM','--kill-after=10s','120s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(out)+':/results:rw','-v',str(out)+':/probe:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',image,*args];(out/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
def qmp():
 fd=os.open(out,os.O_DIRECTORY|os.O_RDONLY)
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
   assert x['return']=={'enabled':True,'present':True};return x['return']
  finally:s.close()
 finally:os.close(fd)
start=time.monotonic();kvm=None
with(out/'console.log').open('wb')as log:
 p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
 while p.poll()is None:
  if kvm is None:
   try:kvm=qmp()
   except(OSError,ValueError):pass
  time.sleep(.05)
 rc=p.wait()
clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(out/'cleanup.json').write_text(json.dumps({'rc':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr},indent=2)+'\n')
text=(out/'console.log').read_text(errors='replace');notes=re.findall(r'^ARRAY_HELPER_NOTES=([^\r\n]+)',text,re.M);releases=re.findall(r'^ARRAY_HELPER_RELEASE=([^\r\n]+)',text,re.M);exits=re.findall(r'^ARRAY_HELPER_GUEST_EXIT=(\d+)\r?$',text,re.M);records={};errors=[];(out/'reports').mkdir(exist_ok=True)
for n,h,b in re.findall(r'^ARRAY_HELPER_FILE=([^:\r\n]+):([a-f0-9]{64}):([A-Za-z0-9+/=]+)\r?$',text,re.M):
 try:
  assert n==Path(n).name and n not in records;raw=base64.b64decode(b,validate=True);assert hashlib.sha256(raw).hexdigest()==h;records[n]=json.loads(raw);(out/'reports'/n).write_bytes(raw)
 except Exception as e:errors.append(str(e))
roots=re.findall(r'^--- PASS: (TestNativeArrayPopulateKernelComponent) \(',text,re.M);subs=re.findall(r'^\s{4,}--- PASS: (TestNativeArrayPopulateKernelComponent/\S+) \(',text,re.M)
passed=rc==0 and clean.returncode==0 and kvm=={'enabled':True,'present':True} and exits==['0'] and notes==['7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828'] and releases==['6.12.26-william-normal-split-v1-e210f5e3ee94'] and len(roots)==1 and len(subs)==4 and len(records)==4 and not errors
result={'id':'NEON-S-ARRAY-HELPER-001','additional_case_id':'NEON-S-ARRAY-DURABILITY-001','status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT','host_qemu_exit':rc,'actual_guest_exits':exits,'actual_qmp_kvm':kvm,'seconds':time.monotonic()-start,'roots':roots,'subcases':subs,'reports':list(records),'errors':errors,'go_binary_sha256':'0798c0637789300cdb3745a1f6e2ff644087494d96fd008c44f9dad37b8c2fc4','helper_ELF_sha256':'a186e9e072f052d33fb6598d6a8fc81235eef093ce9ef7697684dc3a5b5af157','console_sha256':hashlib.sha256((out/'console.log').read_bytes()).hexdigest(),'all_sources_closed':False,'normal_authorized':False,'PG_started':False,'physical_free_proved':False};(out/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True);raise SystemExit(0 if passed else 1)
