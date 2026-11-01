from pathlib import Path
import os,sys,stat,gzip,json,hashlib,shutil,subprocess,time,socket,re,base64
p=Path(__file__).parent;b=Path(sys.argv[1]);c=Path(sys.argv[2]);root=p/'initramfs-root';root.mkdir()
sha=lambda q:hashlib.sha256(q.read_bytes()).hexdigest()
def put(name,obj):(p/name).write_text(json.dumps(obj,indent=2)+'\n')
for d in ['bin','sbin','lib','dev','proc','sys','run','tmp','etc/udev/rules.d','neonvm/acpi','neonvm/bin']:(root/d).mkdir(parents=True,exist_ok=True)
old=Path('/data/neon-build/validation/pg17-s-kernel-001-20261030T154132Z/initramfs-root')
shutil.copyfile(old/'bin/busybox',root/'bin/busybox');os.chmod(root/'bin/busybox',0o755)
# 全部程序/loader来自这次精确源码编译和已验签runtime APK；不是旧镜像的同名替身。
for f in (b/'guest-libs').iterdir():shutil.copyfile(f,root/'lib'/f.name);os.chmod(root/'lib'/f.name,0o755)
for src,dst in [('eudev-root/sbin/udevd','sbin/udevd'),('eudev-root/bin/udevadm','bin/udevadm'),('acpid-root/sbin/acpid','sbin/acpid')]:shutil.copyfile(b/'export'/src,root/dst);os.chmod(root/dst,0o755)
for d in ['lib/udev','etc/udev']:
 src=b/'export/eudev-root'/d
 if src.exists():shutil.copytree(src,root/d,symlinks=True,dirs_exist_ok=True)
shutil.copyfile(c/'export/ordinary-monitor',root/'ordinary-monitor');os.chmod(root/'ordinary-monitor',0o755)
(root/'etc/passwd').write_text('root:x:0:0:root:/:/bin/sh\nneon-udev:x:60004:60004:udev:/:/bin/false\nneon-udev-init:x:60005:60005:init:/:/bin/false\nneon-acpi:x:60006:60006:acpi:/:/bin/false\nneon-logs:x:60007:60007:logs:/:/bin/false\n')
(root/'etc/group').write_text('root:x:0:\ntty:x:5:\ndialout:x:20:\ndisk:x:6:\nkmem:x:9:\nlp:x:7:\ncdrom:x:11:\ntape:x:26:\nkvm:x:34:\ninput:x:24:\nvideo:x:27:\naudio:x:28:\nneon-udev:x:60004:\nneon-udev-init:x:60005:\nneon-acpi:x:60006:\nneon-logs:x:60007:\n')
(root/'neonvm/acpi/vmacpi').write_text('event=button/power\naction=/neonvm/bin/poweroff\n')
# 该业务alias原来由udev规则生成，本显式fixed-device profile由Root先准备。
(root/'etc/udev/rules.d/99-neon-log.rules').write_text('SUBSYSTEM=="virtio-ports", ATTR{name}=="tech.neon.log.0", SYMLINK+="virtio-ports/$attr{name}", ENV{NEON_FIXED_TEST}="executed"\n')
(root/'init').write_text("""#!/bin/busybox sh
set -eu
export PATH=/bin:/sbin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t tmpfs -o mode=0755 tmpfs /run
echo KERNEL_RELEASE=$(uname -r)
echo KERNEL_NOTES_BEGIN
base64 /sys/kernel/notes
echo KERNEL_NOTES_END
mkdir -p /run/udev /run/neon-dsga /run/neon-ordinary/acpi /dev/virtio-ports
chown 60004:60004 /run/udev
chmod 0700 /run/udev
chown 60006:60006 /run/neon-ordinary/acpi
chmod 0700 /run/neon-ordinary/acpi
chmod 0755 /run/neon-dsga
# 原发行版规则已有且存在的静态节点，Root落实其原mode/gid后普通udev只校验。
test ! -c /dev/loop-control || { chown 0:6 /dev/loop-control; chmod 0660 /dev/loop-control; }
for node in /dev/kvm /dev/vhost-net /dev/vhost-vsock; do
 test ! -c "$node" || { chown 0:34 "$node"; chmod 0666 "$node"; }
done
for node in /dev/net/tun /dev/fuse /dev/vfio/vfio; do
 test ! -c "$node" || { chown 0:0 "$node"; chmod 0666 "$node"; }
done
found=0
for port in /sys/class/virtio-ports/*; do
  test -f "$port/name" || continue
  if test "$(cat "$port/name")" = tech.neon.log.0; then
    node=/dev/$(basename "$port")
    test -c "$node"
    chown 0:60007 "$node"
    chmod 0660 "$node"
    ln -s "../$(basename "$node")" /dev/virtio-ports/tech.neon.log.0
    found=1
  fi
done
test "$found" = 1
for event in /dev/input/event*; do
 test -c "$event" || continue
 chown 0:60006 "$event"
 chmod 0440 "$event"
done
echo ORDINARY_COMPONENT_TEST_ONLY_NO_GATE
set +e
/ordinary-monitor
rc=$?
echo ORDINARY_GUEST_EXIT=$rc
sync
poweroff -f
""");os.chmod(root/'init',0o755)
entries=[('.',stat.S_IFDIR|0o755,b'',0,0)]
for f in sorted(root.rglob('*')):
 st=f.lstat();rel=str(f.relative_to(root))
 if stat.S_ISDIR(st.st_mode):entries.append((rel,st.st_mode,b'',0,0))
 elif stat.S_ISLNK(st.st_mode):entries.append((rel,st.st_mode,os.readlink(f).encode(),0,0))
 elif stat.S_ISREG(st.st_mode):entries.append((rel,st.st_mode,f.read_bytes(),0,0))
 else:raise RuntimeError('unexpected initramfs node')
entries += [('dev/console',stat.S_IFCHR|0o600,b'',5,1),('dev/null',stat.S_IFCHR|0o666,b'',1,3)]
arc=bytearray()
def add(name,mode,data,ino,major=0,minor=0):
 name=name.encode()+b'\0';fields=[ino,mode,0,0,2 if stat.S_ISDIR(mode)else 1,0,len(data),0,0,major,minor,len(name),0]
 arc.extend(b'070701'+''.join(f'{x:08x}' for x in fields).encode()+name);arc.extend(b'\0'*((-len(arc))%4));arc.extend(data);arc.extend(b'\0'*((-len(arc))%4))
for ino,(name,mode,data,ma,mi)in enumerate(entries,1):add(name,mode,data,ino,ma,mi)
add('TRAILER!!!',0,b'',len(entries)+1);arc.extend(b'\0'*((-len(arc))%512))
with(p/'initramfs.cpio.gz').open('wb')as f:
 with gzip.GzipFile(fileobj=f,filename='',mode='wb',mtime=0)as z:z.write(arc)
put('initramfs-manifest.json',{'archive_sha256':sha(p/'initramfs.cpio.gz'),'files':[{'path':name,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}for name,mode,data,ma,mi in entries]})
k=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export')
assert sha(k/'vmlinuz')=='3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0'
assert sha(k/'kernel.config')=='11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be'
assert sha(k/'kernel-notes.bin')=='7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828'
put('source-lock.json',{'scope':'TEST_ONLY_ACTUAL_PROGRAM_COMPAT_NOT_GATE_SOURCE_NORMAL_OR_E','package_build':str(b),'package_source_lock_sha256':sha(b/'source-lock.json'),'C_probe_build':str(c),'C_probe_source_lock_sha256':sha(c/'source-lock.json'),'C_probe_ELF_sha256':sha(c/'export/ordinary-monitor'),'udevd_sha256':sha(root/'sbin/udevd'),'udevadm_sha256':sha(root/'bin/udevadm'),'acpid_sha256':sha(root/'sbin/acpid'),'runtime_link_lock_sha256':sha(b/'guest-runtime-link-lock.json'),'runtime_APK_signature_exit':0,'Busybox_sha256':sha(root/'bin/busybox'),'kernel_sha256':sha(k/'vmlinuz'),'kernel_notes_sha256':sha(k/'kernel-notes.bin'),'kernel_config_sha256':sha(k/'kernel.config'),'initramfs_sha256':sha(p/'initramfs.cpio.gz'),'Guest_memory_bytes':1<<30,'PG_executed':False,'normal_authorized':False,'E':0})
name=p.name.lower();cmd=['timeout','--signal=TERM','--kill-after=5s','100s','docker','run','--name',name,'--pull=never','--network=none','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges','--device=/dev/kvm:/dev/kvm','--cpus=2','--memory=2g','--memory-swap=2g','--pids-limit=128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(p)+':/proof:rw','-v',str(k)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64','127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e','-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','1024M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/proof/qmp.sock,server=on,wait=off','-device','virtio-serial-pci,id=serial0','-chardev','file,id=log,path=/proof/virtio-log.bin','-device','virtserialport,id=logport,chardev=log,name=tech.neon.log.0','-kernel','/kernel/vmlinuz','-initrd','/proof/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1']
put('qemu-command.json',cmd)
# 唯一QMP会话；只向本进程独立KVM Guest送一次模拟powerbutton，不操作Host电源。
s=None;f=None;actual=None;sent=False;qmpRows=[];start=time.monotonic()
with(p/'console.log').open('wb')as log:
 proc=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
 while proc.poll()is None:
  if s is None:
   try:
    directory=os.open(p,os.O_RDONLY|os.O_DIRECTORY)
    try:
     s=socket.socket(socket.AF_UNIX);s.settimeout(.5);s.connect('/proc/self/fd/'+str(directory)+'/qmp.sock')
    finally:os.close(directory)
    f=s.makefile('rb');json.loads(f.readline())
    for op in ['qmp_capabilities','query-kvm']:
     s.sendall((json.dumps({'execute':op})+'\n').encode())
     while True:
      row=json.loads(f.readline());qmpRows.append(row)
      if 'return'in row or'error'in row:break
     assert'error'not in row
    actual=row['return'];assert actual=={'enabled':True,'present':True}
   except(OSError,ValueError):
    if s:s.close()
    s=None;f=None
  text=(p/'console.log').read_text(errors='replace')
  if actual and not sent and 'ORDINARY_ACPI_READY'in text:
   s.sendall(b'{"execute":"system_powerdown"}\n');sent=True
   while True:
    row=json.loads(f.readline());qmpRows.append(row)
    if 'return'in row or'error'in row:break
   assert 'error'not in row
  time.sleep(.05)
 rc=proc.wait()
if s:s.close()
clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);put('cleanup.json',{'exit':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr});put('qmp.json',qmpRows)
text=(p/'console.log').read_text(errors='replace');notes=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text);actualNotes=base64.b64decode(''.join(notes[1].split()),validate=True)if notes else None
exits=re.findall(r'^ORDINARY_GUEST_EXIT=(\d+)\r?$',text,re.M)
markers=['ORDINARY_EUDEV_PROCESSED_MULTICAST_PASS']
passed=rc==0 and exits==['0'] and actual=={'enabled':True,'present':True} and actualNotes==(k/'kernel-notes.bin').read_bytes() and not sent and all(text.count(x)==1 for x in markers) and 'Kernel panic - not syncing:'not in text
result={'stable_id':'PG17-S-ORDINARY-V2-UDEV-MONITOR-001','status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT_OR_FIXTURE','QEMU_container_exit':rc,'Guest_exit':exits,'actual_kvm':actual,'actual_notes_sha256':hashlib.sha256(actualNotes).hexdigest()if actualNotes else None,'seconds':time.monotonic()-start,'powerbutton_commands':int(sent),'actual_markers':{x:text.count(x)for x in markers},'new_nonRoot_programs':['eudev3.2.14'],'Root_action_server_is_test_fixture':False,'final_Gate_or_SIGNAL_scope_not_installed':True,'PG_executed':False,'normal_authorized':False,'E':0}
put('result.json',result);print(json.dumps(result),flush=True);raise SystemExit(int(not passed))
