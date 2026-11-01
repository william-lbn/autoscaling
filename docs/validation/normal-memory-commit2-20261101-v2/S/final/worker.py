from pathlib import Path
import os,sys,stat,gzip,json,hashlib,shutil,subprocess,time,socket,re,base64
p=Path(__file__).parent
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
shutil.copytree(build/'export/fixture',p/'fixture',symlinks=True)
shutil.copytree(build/'export/fixture/root',root,symlinks=True)
# local export/copytree不保存原uid；落实与原prepare-root相同的唯一PG持久父身份。
os.chown(root/'var/db/postgres',1000,1000)
# 固定DEV账户文件用于实际原Bootstrap的PG身份解析，不执行PG或动态服务。
(root/'etc/passwd').write_text('root:x:0:0:root:/:/neonvm/bin/sh\npostgres:x:1000:1000:PG:/var/db/postgres:/neonvm/bin/sh\n')
(root/'etc/group').write_text('root:x:0:\npostgres:x:1000:\n')
step('mkfs',['docker','run','--rm','--pull=never','--network=none','--cpus=1','--memory=1g','--memory-swap=1g','-v',str(root)+':/rootdisk:ro','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set -eu; truncate -s 256M /proof/root.ext4; mkfs.ext4 -q -F -L fs-origin-component -d /rootdisk /proof/root.ext4'])
step('iso',['docker','run','--rm','--network=none','--pull=never','--cpus=1','--memory=512m','--memory-swap=512m','-v',str(build/'export/fixture/isofiles')+':/isofiles:ro','-v',str(p)+':/proof:rw','--entrypoint','genisoimage','neon-test-only/dsga-kernel-bootstrap-iso:20261031','-quiet','-R','-J','-o','/proof/runtime.iso','/isofiles'])
init=p/'initramfs-root';init.mkdir()
for d in ['bin','lib','dev','proc','sys','newroot']:(init/d).mkdir()
old=Path('/data/neon-build/validation/pg17-s-kernel-001-20261030T154132Z/initramfs-root')
for n in ['bin/busybox','lib/ld-musl-x86_64.so.1']:shutil.copyfile(old/n,init/n);os.chmod(init/n,0o755)
os.symlink('ld-musl-x86_64.so.1',init/'lib/libc.musl-x86_64.so.1')
(init/'init').write_text("""#!/bin/busybox sh
set -eu
export PATH=/bin
export DSGA_MEMORY_COMMIT2_COMPONENT=DEV369_COMMIT8_NOT_NORMAL
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

# Host生产lifecycle负责真实QMP/Unixpeer/pidfd和原唯一Wait；外层不另连其单QMP monitor。
name=p.name.lower()
command=['timeout','--signal=TERM','--kill-after=10s','150s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--cap-add','SYS_PTRACE','--cap-add','KILL','--cap-add','SETUID','--cap-add','SETGID','--cap-add','DAC_OVERRIDE','--user','0:0','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,size=128m,mode=1777','-e','GOMAXPROCS=2','-e','DSGA_INITIAL_HOST_NATIVE=qemu369-v2','-e','DSGA_INITIAL_HOST_OUTPUT=/proof','-v',str(p)+':/proof:rw','-v',str(build/'export/host-memory.test')+':/native-host.test:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/native-host.test',runner,'-test.run=^TestNativeNormalMemoryCommit2Host$','-test.v','-test.count=1','-test.timeout=135s']
put('native-command.json',command)
put('source-lock.json',{'build':str(build),'source_lock_sha256':sha(build/'source-lock.json'),'Host_ELF_sha256':sha(build/'export/host-memory.test'),'Guest_ELF_sha256':sha(build/'export/guest-memory.test'),'vminit_sha256':sha(root/'neonvm/bin/vminit'),'System_manifest_sha256':sha(root/'neonvm/config/dsga-normal-system-source.json'),'runtime_ISO_sha256':sha(p/'runtime.iso'),'root_ext4_before_sha256':sha(p/'root.ext4'),'kernel_image_sha256':sha(kernel/'vmlinuz'),'kernel_config_sha256':sha(kernel/'kernel.config'),'kernel_notes_sha256':sha(kernel/'kernel-notes.bin'),'scope':'ACTUAL_SINGLE8_THEN_FS2_COMPONENT_HOST_API_MODEL_SYSTEM_STANDINS_NOT_PG_NORMAL','normal_authorized':False})
start=time.monotonic()
with(p/'native-host.log').open('wb')as log: rc=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT).returncode
clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);put('cleanup.json',{'exit':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr})
text=(p/'console.log').read_text(errors='replace')if(p/'console.log').exists()else''
notes=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text)
actualNotes=base64.b64decode(''.join(notes[1].split()),validate=True)if notes else None
reports={};errors=[];(p/'reports').mkdir()
for fname,size,digest in re.findall(r'^FS_REPORT_BEGIN=([^:\r\n]+):(\d+):([0-9a-f]{64})\r?$',text,re.M):
 try:
  assert fname in ['filesystem-origin.json','filesystem-diagnostics.json'] and int(size)<=256*1024
  chunks=re.findall(r'^FS_REPORT_CHUNK='+re.escape(fname)+r':(\d+):([A-Za-z0-9+/=]+)\r?$',text,re.M)
  assert [int(i)for i,_ in chunks]==list(range((int(size)+1023)//1024))
  assert len(re.findall(r'^FS_REPORT_END='+re.escape(fname)+r'\r?$',text,re.M))==1
  raw=b''.join(base64.b64decode(b,validate=True)for _,b in chunks)
  assert len(raw)==int(size)and hashlib.sha256(raw).hexdigest()==digest
  (p/'reports'/fname).write_bytes(raw);reports[fname]=digest
 except Exception as e:errors.append('chunked:'+fname+':'+type(e).__name__)
exits=re.findall(r'^FS_GUEST_EXIT=(\d+)\r?$',text,re.M)
hostlog=(p/'native-host.log').read_text(errors='replace')
terminal=json.loads((p/'terminal.json').read_text())if(p/'terminal.json').exists()else None
kvm=json.loads((p/'actual-runas-peer.json').read_text()).get('actual_query_kvm')if(p/'actual-runas-peer.json').exists()else None
scope=json.loads((p/'reports/filesystem-origin.json').read_text())if(p/'reports/filesystem-origin.json').exists()else None
passed=rc==0 and exits==['0'] and actualNotes==(kernel/'kernel-notes.bin').read_bytes()and len(reports)==2 and not errors and'--- PASS: TestNativeNormalMemoryCommit2Host'in hostlog and'NORMAL_MEMORY_COMMIT2_GUEST_SOURCE=PASS_SINGLE8_PORT_CLOSED_FS2_CAPTURED'in text and terminal is not None and terminal['actual_unique_Wait_exit']==0 and kvm=={'enabled':True,'present':True} and scope is not None and scope['port_closed_before_split_and_FS'] and scope['memory_prefix_unchanged'] and scope['actual_selector']==2 and'Kernel panic - not syncing:'not in text
result={'stable_id':'PG17-S-MEMORY-COMMIT2-001','status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT_OR_FIXTURE','Host_container_exit':rc,'Guest_exits':exits,'actual_kvm':kvm,'seconds':time.monotonic()-start,'reports':reports,'report_errors':errors,'actual_notes_sha256':hashlib.sha256(actualNotes).hexdigest()if actualNotes else None,'Host_ELF_sha256':sha(build/'export/host-memory.test'),'Guest_ELF_sha256':sha(build/'export/guest-memory.test'),'actual_single_commit8_then_FS2':bool(passed),'Host_API_is_model':True,'System_services_are_standins':True,'PG_executed':False,'normal_authorized':False,'E':0}
put('result.json',result);print(json.dumps(result),flush=True);raise SystemExit(int(not passed))
