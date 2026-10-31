from pathlib import Path
import json,subprocess,hashlib,shutil,os,stat,gzip,socket,time,re,base64
p=Path(__file__).parent;root=p/'rootdisk';tools='127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1';runner='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
def step(label,cmd):
 (p/(label+'-command.json')).write_text(json.dumps(cmd,indent=2)+'\n')
 with (p/(label+'.log')).open('wb')as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
 (p/(label+'.exitcode')).write_text(str(r.returncode)+'\n');assert r.returncode==0,(label,r.returncode)
step('compile',['docker','run','--rm','--pull=never','--network','none','--memory','1g','--memory-swap','1g','--cpus','1','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set -eu; gcc-12 --version >/proof/compiler.txt; gcc-12 -static -O2 -Wall -Wextra -Werror -o /proof/guard-diag /proof/guard-diag.c; readelf -l /proof/guard-diag >/proof/guard-readelf.txt; ! grep -q INTERP /proof/guard-readelf.txt'])
shutil.copy2(p/'guard-diag',root/'neonvm/bin/dsga-resource-guard');os.chmod(root/'neonvm/bin/dsga-resource-guard',0o755)
step('prepare-root',['docker','run','--rm','--pull=never','--network','none','--memory','1g','--memory-swap','1g','--cpus','1','--cap-drop','ALL','--cap-add','CHOWN','-v',str(root)+':/rootdisk:rw','-v',str(p/'driver')+':/probe:ro','--entrypoint','/rootdisk/neonvm/bin/busybox',runner,'sh','/probe/prepare-root.sh','1000','1000'])
# Only DEV negative fixture: real symlink target is the new tmpfs /tmp.
(root/'var/db/postgres').rmdir();os.symlink('/tmp',root/'var/db/postgres')
step('mkfs',['docker','run','--rm','--pull=never','--network','none','--memory','1g','--memory-swap','1g','--cpus','1','-v',str(root)+':/rootdisk:ro','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set -eu; truncate -s 128M /proof/root.ext4; mkfs.ext4 -q -F -L dsga-mount-fixture -d /rootdisk /proof/root.ext4'])
baseRoot=p/'initramfs-root';baseRoot.mkdir()
old=Path('/data/neon-build/validation/pg17-s-kernel-001-20261030T154132Z/initramfs-root')
for d in ['bin','lib','dev','proc','sys','newroot']:(baseRoot/d).mkdir(parents=True,exist_ok=True)
for n in ['bin/busybox','lib/ld-musl-x86_64.so.1']:shutil.copy2(old/n,baseRoot/n)
shutil.copy2(p/'driver/init',baseRoot/'init');os.chmod(baseRoot/'init',0o755)
entries=[('.',stat.S_IFDIR|0o755,b'',0,0)]
for q in sorted(baseRoot.rglob('*')):
 m=q.lstat().st_mode
 if stat.S_ISDIR(m):entries.append((str(q.relative_to(baseRoot)),stat.S_IFDIR|0o755,b'',0,0))
 elif stat.S_ISREG(m):entries.append((str(q.relative_to(baseRoot)),stat.S_IFREG|stat.S_IMODE(m),q.read_bytes(),0,0))
 else:raise RuntimeError('unexpected initramfs type')
entries += [('dev/console',stat.S_IFCHR|0o600,b'',5,1),('dev/null',stat.S_IFCHR|0o666,b'',1,3)];arc=bytearray()
def add(n,m,d,i,ma=0,mi=0):
 nb=n.encode()+b'\0';fields=[i,m,0,0,2 if stat.S_ISDIR(m)else 1,0,len(d),0,0,ma,mi,len(nb),0];arc.extend(b'070701'+''.join(f'{x:08x}'for x in fields).encode()+nb);arc.extend(b'\0'*((-len(arc))%4));arc.extend(d);arc.extend(b'\0'*((-len(arc))%4))
for i,(n,m,d,ma,mi)in enumerate(entries,1):add(n,m,d,i,ma,mi)
add('TRAILER!!!',0,b'',len(entries)+1);arc.extend(b'\0'*((-len(arc))%512))
with(p/'initramfs.cpio.gz').open('wb')as f:
 with gzip.GzipFile(fileobj=f,mode='wb',filename='',mtime=0)as z:z.write(arc)
(p/'initramfs-manifest.json').write_text(json.dumps({'sha256':hashlib.sha256((p/'initramfs.cpio.gz').read_bytes()).hexdigest(),'files':[{'path':n,'bytes':len(d),'sha256':hashlib.sha256(d).hexdigest()}for n,m,d,ma,mi in entries]},indent=2)+'\n')
kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export');assert hashlib.sha256((kernel/'vmlinuz').read_bytes()).hexdigest()=='3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0'
name=p.name.lower();cmd=['timeout','--signal=TERM','--kill-after=10s','150s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(p)+':/proof:rw','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',runner,'-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','512M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/proof/qmp.sock,server=on,wait=off','-drive','file=/proof/root.ext4,if=virtio,format=raw,cache=none','-drive','file=/proof/runtime.iso,if=virtio,format=raw,readonly=on','-kernel','/kernel/vmlinuz','-initrd','/proof/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=512M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512']
(p/'qemu-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
def qmp():
 fd=os.open(p,os.O_DIRECTORY|os.O_RDONLY)
 try:
  s=socket.socket(socket.AF_UNIX);s.settimeout(.5)
  try:
   s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline());responses=[]
   for op in ['qmp_capabilities','query-kvm','query-version']:
    s.sendall((json.dumps({'execute':op})+'\n').encode())
    while True:
     x=json.loads(f.readline())
     if 'return'in x or'error'in x:break
    assert'error'not in x,x;responses.append({'command':op,'response':x})
   assert responses[1]['response']['return']=={'enabled':True,'present':True};return responses
  finally:s.close()
 finally:os.close(fd)
start=time.monotonic();responses=None
with(p/'console.log').open('wb')as log:
 proc=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
 while proc.poll()is None:
  if responses is None:
   try:responses=qmp()
   except(OSError,ValueError):pass
  time.sleep(.05)
 rc=proc.wait()
ins=subprocess.run(['docker','inspect',name],capture_output=True,text=True);(p/'docker-inspect.json').write_text(ins.stdout)
cleanup=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(p/'cleanup.json').write_text(json.dumps({'exit':cleanup.returncode,'stdout':cleanup.stdout,'stderr':cleanup.stderr},indent=2)+'\n')
text=(p/'console.log').read_text(errors='replace');nt=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text);notes=base64.b64decode(''.join(nt[1].split()),validate=True)if nt else None
roles=re.findall(r'^DEV_ROLE_RESULT=([^:\r\n]+):(\d+)\r?$',text,re.M);initial=re.findall(r'^DEV_VMINIT_EXIT=(\d+)\r?$',text,re.M)
passed=rc==0 and responses is not None and notes==(kernel/'kernel-notes.bin').read_bytes() and initial==['1'] and 'DEV_INITIAL_BOOT_DIAGNOSTIC_ONLY=1'not in text and len(roles)==11 and all(x[1]=='1'for x in roles) and len(re.findall(r'^DEV_CHECK_GUEST_REFUSED=43\r?$',text,re.M))==11 and 'DEV_GUEST_HARNESS_FINISHED=1'in text and 'Kernel panic - not syncing:'not in text
result={'scope':'DEV bad PGDATA-parent symlink early-failure: no real Guard/PG/normal; partial mount state remains Unknown','id':'NEON-S-BUILDER-MOUNT-002','status':'PASS_MECHANISM_ONLY'if passed else'FAIL_MECHANISM','qemu_exit':rc,'actual_qmp':responses,'vminit_exit':initial,'partial_rw_view_retained':'DEV_PARTIAL_RW_VIEW_RETAINED=1'in text,'role_results':roles,'seconds':time.monotonic()-start,'runtime_notes_sha256':hashlib.sha256(notes).hexdigest()if notes else None,'diagnostic_source_sha256':hashlib.sha256((p/'guard-diag.c').read_bytes()).hexdigest(),'diagnostic_binary_sha256':hashlib.sha256((p/'guard-diag').read_bytes()).hexdigest(),'normal_permit':False,'original_guard_executed':False,'PG_executed':False,'E':0}
(p/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
raise SystemExit(int(not passed))
