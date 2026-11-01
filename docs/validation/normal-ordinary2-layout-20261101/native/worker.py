from pathlib import Path
import subprocess,json,hashlib,shutil,os,stat,gzip,time,socket,re,base64
p=Path(__file__).parent
build=Path('/data/neon-build/validation/normal-ordinary2-layout-go-20261101T115946Z-577d62')
tools='127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1'
runner='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
sha=lambda q:hashlib.sha256(q.read_bytes()).hexdigest()
def put(name,v):(p/name).write_text(json.dumps(v,indent=2)+'\n')
def step(label,cmd):
 put(label+'-command.json',cmd)
 with(p/(label+'.log')).open('wb')as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
 (p/(label+'.exitcode')).write_text(str(r.returncode)+'\n');assert r.returncode==0,(label,r.returncode)
root=p/'rootdisk'
shutil.copytree('/data/neon-build/validation/normal-role-ordinary2-build-20261101T105010Z-d8d217/export/fixture/root',root,symlinks=True)
shutil.copyfile(build/'export/layout-native.test',root/'neonvm/bin/dsga-resource-guard');os.chmod(root/'neonvm/bin/dsga-resource-guard',0o755)
shutil.copyfile(build/'source/vm-builder/files/normal-split/vminit',root/'neonvm/bin/vminit')
for rel in ['neonvm/bin/vmstart','neonvm/bin/vmshutdown','neonvm/config/vector.yaml','neonvm/config/chrony.conf']:
 shutil.copyfile(build/'export/assets'/rel,root/rel)
 if '/bin/' in rel:os.chmod(root/rel,0o755)
(root/'etc/inittab').write_text('::sysinit:/neonvm/bin/vminit\n::once:/neonvm/bin/busybox poweroff -f\n')
os.chown(root/'var/db/postgres',1000,1000)
seed_dns=(root/'etc/resolv.conf').read_bytes()
step('mkfs',['docker','run','--rm','--network=none','--cpus=1','--memory=1g','--memory-swap=1g','-v',str(root)+':/rootdisk:ro','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set -eu; truncate -s 512M /proof/root.ext4; mkfs.ext4 -q -F -L ordinary-layout-component -d /rootdisk /proof/root.ext4'])
shutil.copyfile('/data/neon-build/validation/normal-role-ordinary2-native-20261101T105304Z-2f5db5/runtime.iso',p/'runtime.iso')
init=p/'initramfs-root';init.mkdir()
for d in ['bin','lib','dev','proc','sys','newroot']:(init/d).mkdir()
old=Path('/data/neon-build/validation/pg17-s-kernel-001-20261030T154132Z/initramfs-root')
for n in ['bin/busybox','lib/ld-musl-x86_64.so.1']:shutil.copyfile(old/n,init/n);os.chmod(init/n,0o755)
os.symlink('ld-musl-x86_64.so.1',init/'lib/libc.musl-x86_64.so.1')
(init/'init').write_text('#!/bin/busybox sh\nset -eu\nexport PATH=/bin\n/bin/busybox --install -s /bin\nmount -t devtmpfs devtmpfs /dev\nexec </dev/console >/dev/console 2>&1\nmount -t proc proc /proc\nmount -t sysfs sysfs /sys\necho KERNEL_NOTES_BEGIN\nbase64 /sys/kernel/notes\necho KERNEL_NOTES_END\nmount -t ext4 /dev/vda /newroot\nmount --move /dev /newroot/dev\nmount --move /proc /newroot/proc\nmount --move /sys /newroot/sys\nexec /bin/busybox switch_root -c /dev/console /newroot /neonvm/bin/busybox init\n');os.chmod(init/'init',0o755)
entries=[('.',stat.S_IFDIR|0o755,b'',0,0)]
for q in sorted(init.rglob('*')):
 m=q.lstat().st_mode
 if stat.S_ISDIR(m):entries.append((str(q.relative_to(init)),m,b'',0,0))
 elif stat.S_ISLNK(m):entries.append((str(q.relative_to(init)),m,os.readlink(q).encode(),0,0))
 elif stat.S_ISREG(m):entries.append((str(q.relative_to(init)),m,q.read_bytes(),0,0))
entries += [('dev/console',stat.S_IFCHR|0o600,b'',5,1),('dev/null',stat.S_IFCHR|0o666,b'',1,3)]
arc=bytearray()
def add(name,mode,data,i,ma=0,mi=0):
 nb=name.encode()+b'\0';f=[i,mode,0,0,2 if stat.S_ISDIR(mode)else 1,0,len(data),0,0,ma,mi,len(nb),0]
 arc.extend(b'070701'+''.join(f'{x:08x}'for x in f).encode()+nb);arc.extend(b'\0'*((-len(arc))%4));arc.extend(data);arc.extend(b'\0'*((-len(arc))%4))
for i,(name,mode,data,ma,mi)in enumerate(entries,1):add(name,mode,data,i,ma,mi)
add('TRAILER!!!',0,b'',len(entries)+1)
with(p/'initramfs.cpio.gz').open('wb')as f:
 with gzip.GzipFile(fileobj=f,filename='',mode='wb',mtime=0)as z:z.write(arc)
put('initramfs-source.json',{'sha256':sha(p/'initramfs.cpio.gz'),'files':{name:hashlib.sha256(data).hexdigest()for name,mode,data,ma,mi in entries}})
kernel=Path('/data/neon-build/validation/normal-kernel-metadata-build-20261101T023713Z-58603b/build/export')
assert sha(kernel/'vmlinuz').startswith('18c9fd')
name=p.name.lower()
command=['timeout','--signal=TERM','--kill-after=10s','90s','docker','run','--name',name,'--pull=never','--network=none','--read-only','--user','0:0','--cap-drop=ALL','--security-opt=no-new-privileges','--device=/dev/kvm:/dev/kvm','--cpus=2','--memory=3g','--memory-swap=3g','--pids-limit=128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(p)+':/proof:rw','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',runner,'-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','512M,slots=1,maxmem=4G','-object','memory-backend-ram,id=vmem0,size=3584M','-device','virtio-mem-pci,id=vm0,memdev=vmem0,requested-size=0,block-size=8M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/proof/qmp.sock,server=on,wait=off','-drive','file=/proof/root.ext4,if=virtio,format=raw,cache=none','-drive','file=/proof/runtime.iso,if=virtio,format=raw,readonly=on','-kernel','/kernel/vmlinuz','-initrd','/proof/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=512M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512']
put('qemu-command.json',command)
put('source-lock.json',{'build':str(build),'source_lock_sha256':sha(build/'source-lock.json'),'Guest_ELF_sha256':sha(build/'export/layout-native.test'),'Vminit_sha256':sha(root/'neonvm/bin/vminit'),'actual_assets':{rel:sha(build/'export/assets'/rel)for rel in ['neonvm/bin/vmstart','neonvm/bin/vmshutdown','neonvm/config/vector.yaml','neonvm/config/chrony.conf']},'root_before':sha(p/'root.ext4'),'ISO_sha256':sha(p/'runtime.iso'),'kernel_sha256':sha(kernel/'vmlinuz'),'notes_sha256':sha(kernel/'kernel-notes.bin'),'scope':'DEV actual17graph/FD directory/marker mechanism, TestMain diagnostic only, no originalGuard/typedSource/Host8/PG/normal','normal_authorized':False})
responses=None;resources=None
start=time.monotonic()
def query():
 fd=os.open(p,os.O_RDONLY|os.O_DIRECTORY)
 try:
  s=socket.socket(socket.AF_UNIX);s.settimeout(.4);s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline());out=[]
  for op in ['qmp_capabilities','query-kvm','query-memory-size-summary','query-memory-devices']:
   s.sendall((json.dumps({'execute':op})+'\n').encode())
   while True:
    x=json.loads(f.readline())
    if 'return'in x or'error'in x:break
   out.append({'command':op,'response':x})
  s.close();return out
 finally:os.close(fd)
with(p/'console.log').open('wb')as log:
 proc=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
 while proc.poll()is None:
  if responses is None:
   try:responses=query()
   except(OSError,ValueError):pass
  if resources is None:
   ins=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
   if ins.returncode==0:
    obj=json.loads(ins.stdout)[0];pid=obj['State'].get('Pid',0)
    if pid>0:
     try:
      cg=next(x[3:]for x in Path('/proc/'+str(pid)+'/cgroup').read_text().splitlines()if x.startswith('0::'));cp=Path('/sys/fs/cgroup')/cg.lstrip('/')
      resources={'knobs':{k:(cp/k).read_text().strip()for k in ['memory.max','memory.swap.max','cpu.max','pids.max']},'network':obj['HostConfig']['NetworkMode'],'readonly':obj['HostConfig']['ReadonlyRootfs']}
     except(OSError,StopIteration):pass
  time.sleep(.05)
 rc=proc.wait()
ins=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
if ins.returncode==0:
 obj=json.loads(ins.stdout)[0];put('container-terminal.json',{'State':obj['State'],'Image':obj['Image'],'ID':obj['Id']})
cleanup=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);put('cleanup.json',{'exit':cleanup.returncode,'stdout':cleanup.stdout,'stderr':cleanup.stderr})
text=(p/'console.log').read_text(errors='replace');rows=[json.loads(x)for x in re.findall(r'^ORDINARY_LAYOUT_ACTUAL_JSON (\{[^\r\n]+\})\r?$',text,re.M)]
nt=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text);notes=base64.b64decode(''.join(nt[1].split()),validate=True)if nt else None
put('actual-qmp.json',responses);put('actual-resources.json',resources);put('actual-paths.json',rows)
# 在QEMU真实退出后只用debugfs只读回原ext4，不在Host mount业务盘。
step('offline',['docker','run','--rm','--network=none','--cpus=1','--memory=256m','--memory-swap=256m','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set -eu; debugfs -R "cat /etc/resolv.conf" /proof/root.ext4 >/proof/offline-dns 2>/proof/debugfs-dns.log; for p in /var/lib/chrony /var/log/chrony /var/log/vector /etc/resolv.conf /var/lib/neon-dsga/ordinary-layout-component/complete.json; do debugfs -R "stat $p" /proof/root.ext4; done'])
offline=(p/'offline.log').read_text();dns_ok=(p/'offline-dns').read_bytes()==seed_dns
owner_ok=all(re.search(r'User:\s*'+str(uid)+r'\s+Group:\s*'+str(uid),offline)for uid in [60003,60007,60008])
passed=rc==0 and len(rows)==1 and rows[0]['graph_mounts']==17 and rows[0]['stages']==['intent','identity','complete'] and rows[0]['marker_uid']==60001 and rows[0]['source_qualified'] is False and rows[0]['check_guest_denied_before_flock'] and text.count('ORDINARY_LAYOUT_DIAGNOSTIC_EXIT=42')==1 and 'ORDINARY_LAYOUT_ERROR='not in text and notes==(kernel/'kernel-notes.bin').read_bytes() and responses is not None and responses[1]['response'].get('return')=={'enabled':True,'present':True} and dns_ok and owner_ok and resources is not None and resources['knobs']=={'memory.max':str(3<<30),'memory.swap.max':'0','cpu.max':'200000 100000','pids.max':'128'} and 'Kernel panic - not syncing:'not in text
result={'id':'NEON-S-ORDINARY2-DIRECTORY-001','status':'PASS_MECHANISM_ONLY'if passed else'FAIL_MECHANISM_OR_FIXTURE','QEMU_unique_Wait':rc,'Guest_diagnostic_exit42':text.count('ORDINARY_LAYOUT_DIAGNOSTIC_EXIT=42'),'actual_notes_sha256':hashlib.sha256(notes).hexdigest()if notes else None,'actual_graph_count':rows[0]['graph_mounts']if rows else None,'offline_DNS_bytes_unchanged':dns_ok,'offline_owner_checks':bool(owner_ok),'seconds':time.monotonic()-start,'source_qualified':False,'normal':False,'E':0}
put('result.json',result);print(json.dumps(result),flush=True);raise SystemExit(0 if passed else 1)
