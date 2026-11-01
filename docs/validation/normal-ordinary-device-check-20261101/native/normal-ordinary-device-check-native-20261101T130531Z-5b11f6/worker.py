from pathlib import Path
import subprocess,json,hashlib,shutil,os,stat,gzip,time,socket,re,base64
p=Path(__file__).parent;build=Path('/data/neon-build/validation/normal-ordinary-device-check-go-20261101T125524Z-acbdca')
tools='127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1'
runner='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
def put(n,d):(p/n).write_text(json.dumps(d,indent=2)+'\n')
cmd=['docker','run','--rm','--pull=never','--network=none','--read-only','--cpus=1','--memory=512m','--memory-swap=512m','--tmpfs','/tmp:rw,noexec,nosuid,nodev,size=32m','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set -eu; gcc --version > /proof/compiler.txt; gcc -static -O2 -Wall -Wextra -Werror -o /proof/probe /proof/probe.c; readelf -l /proof/probe > /proof/readelf.txt']
put('compile-command.json',cmd)
with(p/'compile.log').open('wb')as log:rc=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT).returncode
(p/'compile.exitcode').write_text(str(rc)+'\n')
if rc!=0:put('result.json',{'status':'BUILD_FAIL','compile_exit':rc});raise SystemExit(1)
init=p/'initramfs-root';init.mkdir()
for d in ['bin','lib','dev','proc','sys','run','neonvm','neonvm/bin']:(init/d).mkdir()
old=Path('/data/neon-build/validation/pg17-s-kernel-001-20261030T154132Z/initramfs-root')
for n in ['bin/busybox','lib/ld-musl-x86_64.so.1']:shutil.copyfile(old/n,init/n);os.chmod(init/n,0o755)
os.symlink('ld-musl-x86_64.so.1',init/'lib/libc.musl-x86_64.so.1')
shutil.copyfile(build/'export/resource-guard',init/'neonvm/bin/dsga-resource-guard');os.chmod(init/'neonvm/bin/dsga-resource-guard',0o755)
shutil.copyfile(p/'probe',init/'probe');os.chmod(init/'probe',0o755)
(init/'init').write_text('#!/bin/busybox sh\nexport PATH=/bin\n/bin/busybox --install -s /bin\nmount -t devtmpfs devtmpfs /dev\nexec </dev/console >/dev/console 2>&1\nmount -t proc proc /proc\nmount -t sysfs sysfs /sys\nmount -t tmpfs -o mode=0755,nosuid,nodev tmpfs /run\necho KERNEL_NOTES_BEGIN\nbase64 /sys/kernel/notes\necho KERNEL_NOTES_END\nsleep 1\n/probe\nr=$?\necho DEVICE_CHECK_GUEST_EXIT=$r\npoweroff -f\n');os.chmod(init/'init',0o755)
entries=[('.',stat.S_IFDIR|0o755,b'',0,0)]
for f in sorted(init.rglob('*')):
 mode=f.lstat().st_mode;data=b''if stat.S_ISDIR(mode)else os.readlink(f).encode()if stat.S_ISLNK(mode)else f.read_bytes();entries.append((str(f.relative_to(init)),mode,data,0,0))
entries += [('dev/console',stat.S_IFCHR|0o600,b'',5,1),('dev/null',stat.S_IFCHR|0o666,b'',1,3)]
arc=bytearray()
def add(name,mode,data,index,ma=0,mi=0):
 nb=name.encode()+b'\0';values=[index,mode,0,0,2 if stat.S_ISDIR(mode)else 1,0,len(data),0,0,ma,mi,len(nb),0];arc.extend(b'070701'+''.join(f'{x:08x}'for x in values).encode()+nb);arc.extend(b'\0'*((-len(arc))%4));arc.extend(data);arc.extend(b'\0'*((-len(arc))%4))
for i,(name,mode,data,ma,mi)in enumerate(entries,1):add(name,mode,data,i,ma,mi)
add('TRAILER!!!',0,b'',len(entries)+1)
with(p/'initramfs.cpio.gz').open('wb')as f:
 with gzip.GzipFile(fileobj=f,filename='',mode='wb',mtime=0)as z:z.write(arc)
put('initramfs-source.json',{'files':{name:hashlib.sha256(data).hexdigest()for name,mode,data,ma,mi in entries},'sha256':sha(p/'initramfs.cpio.gz')})
kernel=Path('/data/neon-build/validation/normal-kernel-metadata-build-20261101T023713Z-58603b/build/export');assert sha(kernel/'vmlinuz').startswith('18c9fd')
put('source-lock.json',{'scope':'ACTUAL_FIXED_CLI_IDENTITY_PIDFS_AND_ACK_MODEL_ROOT_ONLY','build':str(build),'source_lock_sha256':sha(build/'source-lock.json'),'owned6':json.loads((build/'source-lock.json').read_text())['owned6'],'PG_or_Root_Source_not_executed':True,'C_sha256':sha(p/'probe.c'),'C_ELF_sha256':sha(p/'probe'),'CLI_ELF_sha256':sha(build/'export/resource-guard'),'kernel_sha256':sha(kernel/'vmlinuz'),'notes_sha256':sha(kernel/'kernel-notes.bin'),'Root_responder':'MODEL_NO_ROLE_LEASE_OR_COLD_TRIGGER'})
name=p.name.lower()
cmd=['timeout','--signal=TERM','--kill-after=10s','90s','docker','run','--name',name,'--pull=never','--network=none','--read-only','--user','0:0','--cap-drop=ALL','--security-opt=no-new-privileges','--device=/dev/kvm:/dev/kvm','--cpus=2','--memory=3g','--memory-swap=3g','--pids-limit=128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(p)+':/proof:rw','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',runner,'-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','512M,slots=1,maxmem=4G','-object','memory-backend-ram,id=vmem0,size=3584M','-device','virtio-mem-pci,id=vm0,memdev=vmem0,requested-size=0,block-size=8M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/proof/qmp.sock,server=on,wait=off','-kernel','/kernel/vmlinuz','-initrd','/proof/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=512M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512']
put('qemu-command.json',cmd)
qmp=None;resources=None;start=time.monotonic()
def query():
 fd=os.open(p,os.O_RDONLY|os.O_DIRECTORY)
 try:
  s=socket.socket(socket.AF_UNIX);s.settimeout(.4);s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline());out=[]
  for op in ['qmp_capabilities','query-kvm','query-memory-size-summary']:
   s.sendall((json.dumps({'execute':op})+'\n').encode())
   while True:
    item=json.loads(f.readline())
    if'return'in item or'error'in item:break
   out.append({'command':op,'response':item})
  s.close();return out
 finally:os.close(fd)
with(p/'console.log').open('wb')as log:
 process=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
 while process.poll()is None:
  if qmp is None:
   try:qmp=query()
   except(OSError,ValueError):pass
  if resources is None:
   result=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
   if result.returncode==0:
    obj=json.loads(result.stdout)[0];pid=obj['State'].get('Pid',0)
    if pid>0:
     try:
      cg=next(z[3:]for z in Path('/proc/'+str(pid)+'/cgroup').read_text().splitlines()if z.startswith('0::'));d=Path('/sys/fs/cgroup')/cg.lstrip('/');resources={'knobs':{n:(d/n).read_text().strip()for n in ['memory.max','memory.swap.max','cpu.max','pids.max']},'network':obj['HostConfig']['NetworkMode'],'readonly':obj['HostConfig']['ReadonlyRootfs']}
     except(OSError,StopIteration):pass
  time.sleep(.05)
 rc=process.wait()
terminal=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
if terminal.returncode==0:
 obj=json.loads(terminal.stdout)[0];put('container-terminal.json',{'State':obj['State'],'Image':obj['Image'],'ID':obj['Id']})
cleanup=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);put('cleanup.json',{'exit':cleanup.returncode,'stdout':cleanup.stdout,'stderr':cleanup.stderr})
text=(p/'console.log').read_text(errors='replace');matches=re.findall(r'^DEVICE_CHECK_NATIVE_JSON (\{[^\r\n]+\})\r?$',text,re.M);rows=[json.loads(z)for z in matches]
nt=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text);notes=base64.b64decode(''.join(nt[1].split()),validate=True)if nt else None
put('actual-qmp.json',qmp);put('actual-resources.json',resources)
passed=rc==0 and len(rows)==1 and rows[0]['cases']==8 and rows[0]['requests']==5 and text.count('DEVICE_CHECK_GUEST_EXIT=0')==1 and len(re.findall(r'^DEVICE_CHECK_CASE ',text,re.M))==8 and notes==(kernel/'kernel-notes.bin').read_bytes()and qmp is not None and qmp[1]['response'].get('return')=={'enabled':True,'present':True}and resources is not None and resources['knobs']=={'memory.max':str(3<<30),'memory.swap.max':'0','cpu.max':'200000 100000','pids.max':'128'}and'Kernel panic - not syncing:'not in text
result={'id':'PG17-S-ORDINARY2-DEVICE-CHECK-001','status':'PASS_MECHANISM_ONLY'if passed else'FAIL_MECHANISM_OR_FIXTURE','actual_notes_sha256':hashlib.sha256(notes).hexdigest()if notes else None,'QEMU_unique_Wait':rc,'Guest_exit0':text.count('DEVICE_CHECK_GUEST_EXIT=0'),'cases':rows,'seconds':time.monotonic()-start,'source_qualified':False,'normal':False,'E':0}
put('result.json',result);print(json.dumps(result),flush=True);raise SystemExit(0 if passed else 1)
