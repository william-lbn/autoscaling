
from pathlib import Path
import gzip,hashlib,json,os,re,socket,stat,subprocess,time,shutil
r=Path(__file__).parent
assert (r/'compile-exit-code.txt').read_text().strip()=='0'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
write=lambda name,obj:(r/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2)+'\n')
kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export')
tuple_sha={'vmlinuz':'3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0','kernel.config':'11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be','kernel-notes.bin':'7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828'}
assert all(sha(kernel/n)==v for n,v in tuple_sha.items())
qimage='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
x=json.loads(subprocess.check_output(['docker','image','inspect',qimage],text=True))[0];write('qemu-image.json',{'Id':x['Id'],'RepoDigests':x.get('RepoDigests')})
root=r/'initramfs-root';shutil.copytree(r/'export/fixture/root',root,symlinks=True)
for name in ('bin','lib','dev','proc','sys','tmp','var/lib/neon-dsga/state'):(root/name).mkdir(parents=True,exist_ok=True)
old=Path('/data/neon-build/validation/pg17-native-birth-wire-kvm-attempt02-20261030T164834Z/initramfs-root')
for name,h in {'bin/busybox':'672ec674aa77a9699a4313cbf75c6be1d7f2e81c939460954eadab9d36196c6c','lib/ld-musl-x86_64.so.1':'4e5f9d95fbf01b1375090a257faf28d5f00436e20a4f054d1ee80ff5750f166c'}.items():
 assert sha(old/name)==h;(root/name).write_bytes((old/name).read_bytes());(root/name).chmod(0o755)
(root/'lifecycle.test').write_bytes((r/'export/lifecycle.test').read_bytes());(root/'lifecycle.test').chmod(0o755)
elf=subprocess.run(['file',str(root/'lifecycle.test')],text=True,capture_output=True);write('binary-file.json',{'rc':elf.returncode,'stdout':elf.stdout,'stderr':elf.stderr});assert 'statically linked' in elf.stdout
state=r/'state.ext4';f=state.open('wb');f.truncate(64<<20);f.close()
cmd=['/usr/sbin/mkfs.ext4','-q','-F',str(state)];cp=subprocess.run(cmd,text=True,capture_output=True);write('mkfs-command-result.json',{'command':cmd,'rc':cp.returncode,'stdout':cp.stdout,'stderr':cp.stderr});assert cp.returncode==0
init='''#!/bin/busybox sh
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
fail() { echo PG17_SPLIT_GUEST_EXIT=$1; poweroff -f; while true; do sleep 1; done; }
mount -t proc proc /proc || fail 90
mount -t sysfs sysfs /sys || fail 91
mount -t securityfs none /sys/kernel/security || fail 92
mount -t cgroup2 none /sys/fs/cgroup || fail 93
printf '+memory +pids' > /sys/fs/cgroup/cgroup.subtree_control || fail 94
mkdir /sys/fs/cgroup/neon-postgres /sys/fs/cgroup/neon-system || fail 95
printf 67108864 >/sys/fs/cgroup/neon-system/memory.max
printf 0 >/sys/fs/cgroup/neon-system/memory.swap.max
printf 128 >/sys/fs/cgroup/neon-system/pids.max
mkdir -p /neonvm/runtime
mount -t iso9660 -o ro /dev/vdb /neonvm/runtime || fail 98
mount -t ext4 -o noatime /dev/vda /var/lib/neon-dsga/state || fail 96
chmod 700 /var/lib/neon-dsga/state
mount -t tmpfs -o size=16m,nodev,nosuid tmpfs /tmp || fail 99
mount --bind / /
mount -o remount,bind,ro / /
printf 'PG17_SPLIT_RELEASE='; uname -r
printf 'PG17_SPLIT_NOTES_SHA256='; sha256sum /sys/kernel/notes | cut -d ' ' -f 1
echo PG17_SPLIT_NOTES_BASE64_BEGIN; base64 /sys/kernel/notes; echo PG17_SPLIT_NOTES_BASE64_END
printf 'PG17_SPLIT_BINARY_SHA256='; sha256sum /lifecycle.test | cut -d ' ' -f 1
printf 'PG17_SPLIT_CMDLINE='; cat /proc/cmdline
printf 'PG17_SPLIT_BOOT_ID='; cat /proc/sys/kernel/random/boot_id
printf "%s" "$$" >/sys/fs/cgroup/neon-system/cgroup.procs
/bin/busybox env -i PATH=/neonvm/bin /neonvm/bin/dsga-resource-guard system-role-broker
rc=$?
echo PG17_SPLIT_TEST_EXIT=$rc
sync
umount /var/lib/neon-dsga/state || fail 97
echo PG17_SPLIT_GUEST_EXIT=$rc
poweroff -f
'''
(root/'init').write_text(init);(root/'init').chmod(0o755);(r/'guest-init.sh').write_text(init)
entries=[('.',stat.S_IFDIR|0o755,b'',0,0)]
for p in sorted(root.rglob('*')):
 if p.is_symlink():entries.append((str(p.relative_to(root)),stat.S_IFLNK|0o777,os.readlink(p).encode(),0,0))
 elif p.is_dir():entries.append((str(p.relative_to(root)),stat.S_IFDIR|0o755,b'',0,0))
 else:entries.append((str(p.relative_to(root)),stat.S_IFREG|stat.S_IMODE(p.stat().st_mode),p.read_bytes(),0,0))
entries.extend([('dev/console',stat.S_IFCHR|0o600,b'',5,1),('dev/null',stat.S_IFCHR|0o666,b'',1,3)])
a=bytearray()
def add(name,mode,data,ino,ma=0,mi=0):
 n=name.encode()+b'\0';fields=[ino,mode,0,0,2 if stat.S_ISDIR(mode) else 1,0,len(data),0,0,ma,mi,len(n),0];a.extend(b'070701'+''.join(f'{x:08x}' for x in fields).encode()+n);a.extend(b'\0'*((-len(a))%4));a.extend(data);a.extend(b'\0'*((-len(a))%4))
for i,(n,m,b,ma,mi) in enumerate(entries,1):add(n,m,b,i,ma,mi)
add('TRAILER!!!',0,b'',len(entries)+1);a.extend(b'\0'*((-len(a))%512))
with (r/'initramfs.cpio.gz').open('wb') as f:
 with gzip.GzipFile(fileobj=f,mode='wb',mtime=0,filename='') as g:g.write(a)
write('initramfs-manifest.json',{'files':[{'path':n,'mode':oct(m),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest() if stat.S_ISREG(m) else None} for n,m,b,ma,mi in entries],'gzipSHA256':sha(r/'initramfs.cpio.gz')})
name='split-topology-kvm-'+r.name[-18:].lower();qmp=r/'qmp.sock'
cmdline='console=ttyS0,115200 earlycon=uart8250,io,0x3f8,115200n8 rdinit=/init panic=-1 kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512 pg17_split_topology_component=1'
qemu=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','1024M','-nographic','-monitor','none','-qmp','unix:/evidence/qmp.sock,server=on,wait=off','-no-reboot','-no-user-config','-nic','none','-kernel','/kernel/vmlinuz','-initrd','/evidence/initramfs.cpio.gz','-drive','file=/evidence/state.ext4,format=raw,if=virtio','-drive','file=/evidence/runtime.iso,format=raw,if=virtio,readonly=on','-append',cmdline]
cmd=['docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--ulimit','core=0','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(r)+':/evidence:rw','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',qimage,*qemu]
write('qemu-command.json',cmd);messages=[];kvm=None;start=time.monotonic();timeout=False
try:
 with (r/'console.log').open('wb') as f:
  p=subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT);until=time.monotonic()+10
  while not qmp.exists() and p.poll() is None and time.monotonic()<until:time.sleep(.02)
  if qmp.exists():
   s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);s.settimeout(3);dirfd=os.open(r,os.O_DIRECTORY);s.connect('/proc/self/fd/'+str(dirfd)+'/qmp.sock');os.close(dirfd);st=s.makefile('rwb');messages.append(json.loads(st.readline()))
   for method,ident in [('qmp_capabilities','caps'),('query-kvm','kvm'),('query-version','version')]:
    st.write((json.dumps({'execute':method,'id':ident})+'\n').encode());st.flush()
    while True:
     m=json.loads(st.readline());messages.append(m)
     if m.get('id')==ident:
      if ident=='kvm':kvm=m.get('return')
      break
   st.close();s.close()
  try:rc=p.wait(timeout=75)
  except subprocess.TimeoutExpired:timeout=True;subprocess.run(['docker','kill',name],capture_output=True);rc=p.wait(timeout=10)
finally:
 write('qmp-transcript.json',messages);i=subprocess.run(['docker','inspect',name],text=True,capture_output=True)
 if i.returncode==0:
  x=json.loads(i.stdout)[0];write('qemu-container.json',{'Id':x['Id'],'Image':x['Image'],'State':x['State'],'NetworkMode':x['HostConfig']['NetworkMode'],'Memory':x['HostConfig']['Memory'],'NanoCpus':x['HostConfig']['NanoCpus']})
 clean=subprocess.run(['docker','rm','-f',name],text=True,capture_output=True);write('cleanup.json',{'rc':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr})
text=(r/'console.log').read_text(errors='replace').replace('\r','');ids=re.findall(r'^(NEON-S-ROLE-GATE-00[6-7]) PASS[^\n]*$',text,re.M)
release=re.findall(r'^PG17_SPLIT_RELEASE=(.+)$',text,re.M);binary=re.findall(r'^PG17_SPLIT_BINARY_SHA256=([a-f0-9]{64})$',text,re.M);guest=re.findall(r'^PG17_SPLIT_GUEST_EXIT=(\d+)$',text,re.M);test=re.findall(r'^PG17_SPLIT_TEST_EXIT=(\d+)$',text,re.M)
notes=re.search(r'^PG17_SPLIT_NOTES_BASE64_BEGIN\n(.*?)\nPG17_SPLIT_NOTES_BASE64_END$',text,re.M|re.S)
actualnotes=None
if notes:
 import base64
 b=base64.b64decode(notes[1]);(r/'guest-notes.bin').write_bytes(b);actualnotes=hashlib.sha256(b).hexdigest()
component=re.findall(r'^ROLE_GATE_RESULT=(.+)$',text,re.M)
if len(component)==1:write('actual-guest-component.json',json.loads(component[0]))
passed=rc==0 and not timeout and kvm=={'enabled':True,'present':True} and release==['6.12.26-william-normal-split-v1-e210f5e3ee94'] and actualnotes==tuple_sha['kernel-notes.bin'] and binary==[sha(r/'export/lifecycle.test')] and guest==['0'] and test==['0'] and ids==[f'NEON-S-ROLE-GATE-{i:03d}' for i in range(6,8)] and len(component)==1 and 'Kernel panic' not in text
fsck=subprocess.run(['/usr/sbin/e2fsck','-f','-n',str(state)],text=True,capture_output=True);write('state-fsck.json',{'rc':fsck.returncode,'stdout':fsck.stdout,'stderr':fsck.stderr})
result={'status':'PASS_COMPONENT_ONLY' if passed else 'FAIL_COMPONENT_OR_INFRA','pass':passed,'rc':rc,'timeout':timeout,'seconds':time.monotonic()-start,'actualKVM':kvm,'GuestMemoryMiB':1024,'GuestCPUs':2,'kernelTuple':tuple_sha,'guestNotesSHA256':actualnotes,'GuestRelease':release,'GuestExit':guest,'TestExit':test,'caseIDs':ids,'testBinarySHA256':sha(r/'export/lifecycle.test'),'stateDiskSHA256':sha(state),'fsckExit':fsck.returncode,'normalDecreasePermit':False,'notProven':['production fixed ISO loader and original Source issuance','Root Birth/Enrollment','continuous force/I/K envelope and no added OOM','safe normal decrease','real PG workload','Kubernetes E2E']};write('result.json',result);print(json.dumps(result,ensure_ascii=False,indent=2),flush=True)
raise SystemExit(0 if passed else 1)
