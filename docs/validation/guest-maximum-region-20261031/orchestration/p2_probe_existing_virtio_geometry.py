import subprocess
REMOTE=r"""
from pathlib import Path
import datetime,hashlib,json,subprocess,stat,gzip
b=Path('/data/neon-build');nonce=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=b/'validation'/('virtio-geometry-existing-'+nonce);out.mkdir();root=out/'initramfs-root';root.mkdir()
for d in ['bin','lib','dev','proc','sys']:(root/d).mkdir(parents=True,exist_ok=True)
old=b/'validation/pg17-s-kernel-001-20261030T154132Z/initramfs-root'
for n,h in [('bin/busybox','672ec674aa77a9699a4313cbf75c6be1d7f2e81c939460954eadab9d36196c6c'),('lib/ld-musl-x86_64.so.1','4e5f9d95fbf01b1375090a257faf28d5f00436e20a4f054d1ee80ff5750f166c')]:
 raw=(old/n).read_bytes();assert hashlib.sha256(raw).hexdigest()==h;(root/n).write_bytes(raw);(root/n).chmod(0o755)
init='''#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
mount -t proc proc /proc
mount -t sysfs sysfs /sys
echo EXISTING_VIRTIO_GEOMETRY_BEGIN
cat /proc/sys/kernel/osrelease
echo KERNEL_NOTES_BEGIN
base64 /sys/kernel/notes
echo KERNEL_NOTES_END
echo IOMEM_BEGIN
cat /proc/iomem
echo IOMEM_END
for device in /sys/bus/virtio/devices/*; do
 echo VIRTIO_DEVICE_BEGIN
 readlink -f "$device"
 cat "$device/device" "$device/vendor" "$device/status" "$device/features"
 readlink -f "$device/driver"
 ls -la "$device/"
 echo VIRTIO_DEVICE_END
done
echo MEMORY_ONLINE_BEGIN
for block in /sys/devices/system/memory/memory*/state; do
 printf '%s=' "$block"
 cat "$block"
done
cat /sys/devices/system/node/online
echo MEMORY_ONLINE_END
echo EXISTING_VIRTIO_GEOMETRY_GUEST_EXIT=0
sync
poweroff -f
''';(root/'init').write_text(init);(root/'init').chmod(0o755);(out/'guest-init.sh').write_text(init)
entries=[('.',stat.S_IFDIR|0o755,b'',0,0)]
for p in sorted(root.rglob('*')):entries.append((str(p.relative_to(root)),(stat.S_IFDIR|0o755)if p.is_dir()else(stat.S_IFREG|stat.S_IMODE(p.stat().st_mode)),b''if p.is_dir()else p.read_bytes(),0,0))
entries.extend([('dev/console',stat.S_IFCHR|0o600,b'',5,1),('dev/null',stat.S_IFCHR|0o666,b'',1,3)]);arc=bytearray()
def add(n,m,d,i,ma=0,mi=0):
 nb=n.encode()+b'\0';fields=[i,m,0,0,2 if stat.S_ISDIR(m) else 1,0,len(d),0,0,ma,mi,len(nb),0];arc.extend(b'070701'+''.join(f'{x:08x}'for x in fields).encode()+nb);arc.extend(b'\0'*((-len(arc))%4));arc.extend(d);arc.extend(b'\0'*((-len(arc))%4))
for i,(n,m,d,ma,mi)in enumerate(entries,1):add(n,m,d,i,ma,mi)
add('TRAILER!!!',0,b'',len(entries)+1);arc.extend(b'\0'*((-len(arc))%512))
with(out/'initramfs.cpio.gz').open('wb')as f:
 with gzip.GzipFile(fileobj=f,mode='wb',filename='',mtime=0)as z:z.write(arc)
(out/'initramfs-manifest.json').write_text(json.dumps({'gzip_sha256':hashlib.sha256((out/'initramfs.cpio.gz').read_bytes()).hexdigest(),'files':[{ 'path':n,'bytes':len(d),'sha256':hashlib.sha256(d).hexdigest()if stat.S_ISREG(m)else None}for n,m,d,ma,mi in entries]},indent=2)+'\n')
runner=r'''from pathlib import Path
import subprocess,json,socket,time,os,hashlib,re,base64
out=Path(OUT);kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export');assert hashlib.sha256((kernel/'vmlinuz').read_bytes()).hexdigest()=='3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0'
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e';name=out.name.lower()
args=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','size=256M,slots=1,maxmem=1G','-object','memory-backend-ram,id=vmem0,size=768M','-device','virtio-mem-pci,id=vm0,memdev=vmem0,block-size=8M,requested-size=0','-uuid','11111111-2222-3333-4444-555555555555','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/results/qmp.sock,server=on,wait=off','-kernel','/kernel/vmlinuz','-initrd','/probe/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512']
cmd=['timeout','--signal=TERM','--kill-after=10s','90s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(out)+':/results:rw','-v',str(out)+':/probe:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/usr/bin/qemu-system-x86_64',image,*args];(out/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
def qmp():
 fd=os.open(out,os.O_DIRECTORY|os.O_RDONLY)
 try:
  s=socket.socket(socket.AF_UNIX);s.settimeout(.5)
  try:
   s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');json.loads(f.readline());result=[]
   for op,argument in [('qmp_capabilities',None),('query-kvm',None),('query-version',None),('qom-get',{'path':'/machine','property':'memory'}),('query-memory-devices',None),('query-memdev',None)]:
    req={'execute':op};
    if argument is not None:req['arguments']=argument
    s.sendall((json.dumps(req)+'\n').encode())
    while True:
     x=json.loads(f.readline())
     if 'return'in x or'error'in x:break
    assert'error'not in x,x;result.append({'command':req,'response':x})
   assert result[1]['response']['return']=={'enabled':True,'present':True};return result
  finally:s.close()
 finally:os.close(fd)
start=time.monotonic();responses=None
with(out/'console.log').open('wb')as log:
 p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
 while p.poll()is None:
  if responses is None:
   try:responses=qmp()
   except(OSError,ValueError):pass
  time.sleep(.05)
 rc=p.wait()
clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(out/'cleanup.json').write_text(json.dumps({'rc':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr},indent=2)+'\n');text=(out/'console.log').read_text(errors='replace');notes=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text);raw=base64.b64decode(''.join(notes[1].split()),validate=True)if notes else None;passed=rc==0 and responses is not None and'EXISTING_VIRTIO_GEOMETRY_GUEST_EXIT=0'in text and raw==(kernel/'kernel-notes.bin').read_bytes()and'Kernel panic - not syncing:'not in text
result={'status':'PASS_READONLY_INTERFACE_PROBE'if passed else'FAIL_PROBE','host_qemu_exit':rc,'elapsed_seconds':time.monotonic()-start,'qmp':responses,'kernel_notes_sha256':hashlib.sha256(raw).hexdigest()if raw else None,'console_sha256':hashlib.sha256((out/'console.log').read_bytes()).hexdigest(),'normal_permit':False,'all_sources_closed':False,'PG_started':False};(out/'result.json').write_text(json.dumps(result,indent=2)+'\n');(out/'exitcode').write_text(('0'if passed else'1')+'\n');print(json.dumps(result),flush=True);raise SystemExit(int(not passed))
'''.replace('Path(OUT)','Path('+repr(str(out))+')');(out/'run.py').write_text(runner);unit='virtio-geometry-existing-'+nonce.lower();subprocess.run(['systemd-run','--unit',unit,'--property=Type=oneshot','--property=StandardOutput=file:'+str(out/'runner.log'),'--property=StandardError=file:'+str(out/'runner.log'),'--no-block','python3',str(out/'run.py')],check=True);(b/'validation/virtio-geometry-existing-latest.txt').write_text(str(out)+'\n');print(str(out),unit)
"""
subprocess.run(['python3','remote.py','python3 -'],input=REMOTE,text=True,check=True)
