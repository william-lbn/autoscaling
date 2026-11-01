from pathlib import Path
import os,sys,stat,gzip,json,hashlib,shutil,subprocess,time,socket,re,base64
p=Path(__file__).parent/'native-run';p.mkdir()
build=Path(sys.argv[1]);root=p/'rootdisk'
sha=lambda q:hashlib.sha256(q.read_bytes()).hexdigest()
tools='127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1'
runner='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
def put(name,obj):
 q=p/name;q.parent.mkdir(parents=True,exist_ok=True);q.write_text(json.dumps(obj,indent=2)+'\n')
def step(label,cmd):
 put(label+'-command.json',cmd)
 with(p/(label+'.log')).open('wb')as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
 (p/(label+'.exitcode')).write_text(str(r.returncode)+'\n');assert r.returncode==0,(label,r.returncode)
shutil.copytree(build/'export/fixture/root',root,symlinks=True)
# local export/copytree不保存原uid；落实与原prepare-root相同的唯一PG持久父身份。
os.chown(root/'var/db/postgres',1000,1000)
# 固定DEV账户文件用于实际原Bootstrap的PG身份解析，不执行PG或动态服务。
(root/'etc/passwd').write_text('root:x:0:0:root:/:/neonvm/bin/sh\npostgres:x:1000:1000:PG:/var/db/postgres:/neonvm/bin/sh\n')
(root/'etc/group').write_text('root:x:0:\npostgres:x:1000:\n')
step('mkfs',['docker','run','--rm','--pull=never','--network=none','--cpus=1','--memory=1g','--memory-swap=1g','-v',str(root)+':/rootdisk:ro','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set -eu; truncate -s 512M /proof/root.ext4; mkfs.ext4 -q -F -L fs-origin-component -d /rootdisk /proof/root.ext4'])
step('iso',['docker','run','--rm','--network=none','--pull=never','--cpus=1','--memory=512m','--memory-swap=512m','-v',str(build/'export/fixture/isofiles')+':/isofiles:ro','-v',str(p)+':/proof:rw','--entrypoint','genisoimage','sha256:cb6635e1790c7ceae132b6a75a61e85e33be1fde8dbd9c421a17a33cd2494b9d','-quiet','-R','-J','-o','/proof/runtime.iso','/isofiles'])
init=p/'initramfs-root';init.mkdir()
for d in ['bin','lib','dev','proc','sys','newroot']:(init/d).mkdir()
old=Path('/data/neon-build/validation/pg17-s-kernel-001-20261030T154132Z/initramfs-root')
for n in ['bin/busybox','lib/ld-musl-x86_64.so.1']:shutil.copyfile(old/n,init/n);os.chmod(init/n,0o755)
os.symlink('ld-musl-x86_64.so.1',init/'lib/libc.musl-x86_64.so.1')
(init/'init').write_text("""#!/bin/busybox sh
set -eu
export PATH=/bin
export DSGA_FS_COMPONENT=DEV369_FILESYSTEM_ORIGIN
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
mount -t proc proc /proc
mount -t sysfs sysfs /sys
echo KERNEL_RELEASE=$(uname -r)
echo KERNEL_NOTES_BEGIN
base64 /sys/kernel/notes
echo KERNEL_NOTES_END
mount -t ext4 /dev/vda /newroot
mount --move /dev /newroot/dev
mount --move /proc /newroot/proc
mount --move /sys /newroot/sys
# 真正PID1切根，不用chroot子进程把initrd PID1伪称同root。
exec /bin/busybox switch_root -c /dev/console /newroot /neonvm/bin/busybox init
""");os.chmod(init/'init',0o755)
entries=[('.',stat.S_IFDIR|0o755,b'',0,0)]
for q in sorted(init.rglob('*')):
 mode=q.lstat().st_mode
 if stat.S_ISDIR(mode):entries.append((str(q.relative_to(init)),stat.S_IFDIR|0o755,b'',0,0))
 elif stat.S_ISLNK(mode):entries.append((str(q.relative_to(init)),stat.S_IFLNK|0o777,os.readlink(q).encode(),0,0))
 elif stat.S_ISREG(mode):entries.append((str(q.relative_to(init)),mode,q.read_bytes(),0,0))
 else:raise RuntimeError('unexpected initramfs file')
entries += [('dev/console',stat.S_IFCHR|0o600,b'',5,1),('dev/null',stat.S_IFCHR|0o666,b'',1,3)]
arc=bytearray()
def add(name,mode,data,ino,major=0,minor=0):
 n=name.encode()+b'\0';f=[ino,mode,0,0,2 if stat.S_ISDIR(mode)else 1,0,len(data),0,0,major,minor,len(n),0]
 arc.extend(b'070701'+''.join(f'{x:08x}'for x in f).encode()+n);arc.extend(b'\0'*((-len(arc))%4));arc.extend(data);arc.extend(b'\0'*((-len(arc))%4))
for ino,(name,mode,data,ma,mi)in enumerate(entries,1):add(name,mode,data,ino,ma,mi)
add('TRAILER!!!',0,b'',len(entries)+1);arc.extend(b'\0'*((-len(arc))%512))
with(p/'initramfs.cpio.gz').open('wb')as f:
 with gzip.GzipFile(fileobj=f,filename='',mode='wb',mtime=0)as z:z.write(arc)
put('initramfs-manifest.json',{'sha256':sha(p/'initramfs.cpio.gz'),'files':[{'path':name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}for name,mode,data,ma,mi in entries]})
kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export')
assert sha(kernel/'vmlinuz')=='3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0'
required=(build/'export/fixture/required-kernel-args.txt').read_text().strip()
cmdline='console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=2048M '+required
name=build.name.lower()+'-guest'
cmd=['timeout','--signal=TERM','--kill-after=10s','180s','docker','run','--name',name,'--pull=never','--network=none','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges','--device=/dev/kvm:/dev/kvm','--cpus=2','--memory=3g','--memory-swap=3g','--pids-limit=128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(p)+':/proof:rw','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',runner,'-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','2048M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/proof/qmp.sock,server=on,wait=off','-drive','file=/proof/root.ext4,if=virtio,format=raw,cache=none','-drive','file=/proof/runtime.iso,if=virtio,format=raw,readonly=on','-kernel','/kernel/vmlinuz','-initrd','/proof/initramfs.cpio.gz','-append',cmdline]
put('qemu-command.json',cmd)
put('source-lock.json',{'build_cohort':str(build),'Go_source_lock_sha256':sha(build/'source-lock.json'),'Go_test_ELF_sha256':sha(build/'export/service.test'),'vminit_sha256':sha(root/'neonvm/bin/vminit'),'System_manifest_sha256':sha(root/'neonvm/config/dsga-normal-system-source.json'),'runtime_ISO_sha256':sha(p/'runtime.iso'),'root_ext4_before_sha256':sha(p/'root.ext4'),'kernel_image_sha256':sha(kernel/'vmlinuz'),'kernel_config_sha256':sha(kernel/'kernel.config'),'kernel_notes_sha256':sha(kernel/'kernel-notes.bin'),'Host_Source_fixture':True,'System_services_standins':True,'normal_authorized':False})
def qmp():
 fd=os.open(p,os.O_DIRECTORY|os.O_RDONLY)
 try:
  s=socket.socket(socket.AF_UNIX);s.settimeout(.5)
  try:
   s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline());out=[]
   for op in ['qmp_capabilities','query-kvm']:
    s.sendall((json.dumps({'execute':op})+'\n').encode())
    while True:
     row=json.loads(f.readline())
     if 'return'in row or'error'in row:break
    assert'error'not in row;out.append(row['return'])
   assert out[-1]=={'enabled':True,'present':True};return out[-1]
  finally:s.close()
 finally:os.close(fd)
start=time.monotonic();actual=None
with(p/'console.log').open('wb')as log:
 proc=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
 while proc.poll()is None:
  if actual is None:
   try:actual=qmp()
   except(OSError,ValueError):pass
  time.sleep(.05)
 rc=proc.wait()
inspection=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
if inspection.returncode==0:
 x=json.loads(inspection.stdout)[0];put('container-actual.json',{'Image':x['Image'],'State':x['State'],'Memory':x['HostConfig']['Memory'],'MemorySwap':x['HostConfig']['MemorySwap'],'NanoCpus':x['HostConfig']['NanoCpus'],'NetworkMode':x['HostConfig']['NetworkMode']})
cleanup=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);put('cleanup.json',{'exit':cleanup.returncode,'stdout':cleanup.stdout,'stderr':cleanup.stderr})
text=(p/'console.log').read_text(errors='replace');notes=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text)
actualNotes=base64.b64decode(''.join(notes[1].split()),validate=True)if notes else None
exits=re.findall(r'^ROLE_SERVICE_GUEST_EXIT=(\d+)\r?$',text,re.M);initial=re.findall(r'^ROLE_SERVICE_INITIAL_EXIT=(\d+)\r?$',text,re.M);ids=re.findall(r'^(NEON-S-SYSTEM-ROLE-SERVICE-(?:00[1-7]|011)) PASS[^\r\n]*',text,re.M)
passed=rc==0 and actual=={'enabled':True,'present':True} and actualNotes==(kernel/'kernel-notes.bin').read_bytes() and exits==['0'] and initial==['0'] and sorted(ids)==[f'NEON-S-SYSTEM-ROLE-SERVICE-{i:03d}'for i in (list(range(1,8))+[11])] and 'Kernel panic - not syncing:'not in text and 'ROLE_AUTHORITY_DEV_LEGACY_SERVE_REFUSED'not in text
result={'status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT_OR_INFRA','pass':passed,'Host_QEMU_exit':rc,'Guest_exit':exits,'initial_exit':initial,'caseIDs':ids,'actual_kvm':actual,'seconds':time.monotonic()-start,'actual_notes_SHA256':hashlib.sha256(actualNotes).hexdigest()if actualNotes else None,'Go_test_ELF_sha256':sha(build/'export/service.test'),'production_CLI_SHA256':sha(build/'export/resource-guard.real'),'production_CLI_compiled_not_executed':True,'base_memory_bytes':2<<30,'vCPUs':2,'source_fixture':True,'ordinary9_controls_not_installed_yet':True,'other_System_programs_standins':True,'PG_executed':False,'normal_authorized':False,'E':0};put('result.json',result);print(json.dumps(result),flush=True);raise SystemExit(int(not passed))
