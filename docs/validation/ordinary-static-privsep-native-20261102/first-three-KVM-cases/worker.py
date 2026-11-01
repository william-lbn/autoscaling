from pathlib import Path
import os,sys,stat,gzip,json,hashlib,shutil,subprocess,time,socket,re,base64
p=Path(__file__).parent;meta=json.loads((p/'metadata.json').read_bytes());v=Path('/data/neon-build/validation');build=Path(meta['fixture_build']);product=Path(meta['product_build']);kernel=Path(meta['kernel_export']);sha=lambda q:hashlib.sha256(q.read_bytes()).hexdigest()
def put(q,obj):q.write_text(json.dumps(obj,indent=2)+'\n')
assert json.loads((build/'result.json').read_bytes())['compile_exit']==0
assert sha(product/'export/eudev-root/sbin/udevd')=='c93446d3c232ff55b75980bc7948e9ee5b9a72e37c6a5fddaa9c86d643b71c0a'
assert sha(kernel/'vmlinuz')=='18c9fd3a38bbe5a087b98998c856898e97ee3f0bc68681b13f0287912849e1ad'
assert sha(kernel/'kernel.config')=='966e615127358d6d0dcf4f6ca884a07702e957ad07a95909cf95feca4aae5441'
assert sha(kernel/'kernel-notes.bin')=='a496a3c44fc0b374f100ba0331a2e21f02d6e79ac999769a8d3ad4d71b76024e'
assert sha(build/'export/stock-libudev.so.1')=='6a96205c57a74ec7d6150494d218e906f4656167b6b0ea2dab63b5f61555a795'
libs=v/'ordinary-v2-packages-build-20261101T060652Z-8d5217/guest-libs';busy=v/'pg17-s-kernel-001-20261030T154132Z/initramfs-root/bin/busybox';old=v/'ordinary-udev-privsep-build-20261101T080238Z-e47368';runner='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
def make_initramfs(case,provider,drop):
 root=case/'initramfs-root';root.mkdir()
 for d in ['bin','sbin','lib','dev','proc','sys','run','tmp','etc/udev/rules.d']:(root/d).mkdir(parents=True,exist_ok=True)
 shutil.copyfile(busy,root/'bin/busybox');os.chmod(root/'bin/busybox',0o755)
 for f in libs.iterdir():
  if f.is_file():shutil.copyfile(f,root/'lib'/f.name);os.chmod(root/'lib'/f.name,0o755)
 shutil.copyfile(build/'export/stock-libudev.so.1',root/'lib/libudev.so.1')
 for src,dst in [('sbin/udevd','sbin/udevd'),('bin/udevadm','bin/udevadm')]:shutil.copyfile(provider/'export/eudev-root'/src,root/dst);os.chmod(root/dst,0o755)
 shutil.copyfile(build/'export/static-privsep-native',root/'static-privsep-native');os.chmod(root/'static-privsep-native',0o755)
 (root/'etc/passwd').write_text('root:x:0:0:root:/:/bin/sh\nneon-udev:x:60004:60004:udev:/:/bin/false\n')
 (root/'etc/group').write_text('root:x:0:\ntty:x:5:\ndialout:x:20:\ndisk:x:6:\nkmem:x:9:\nlp:x:7:\ncdrom:x:11:\ntape:x:26:\nkvm:x:34:\ninput:x:24:\nvideo:x:27:\naudio:x:28:\nneon-udev:x:60004:\n')
 # 仅本机制的实际CPU0规则；不载入无关发行版RUN/kmod/device矩阵。
 (root/'etc/udev/rules.d/99-neon-static.rules').write_text('ACTION=="add|change", SUBSYSTEM=="cpu", KERNEL=="cpu0", ENV{NEON_FIXED_TEST}="executed"\n')
 (root/'init').write_text('''#!/bin/busybox sh
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
mkdir -p /run/udev /run/neon-dsga
chown 60004:60004 /run/udev
chmod 0700 /run/udev
chmod 0755 /run/neon-dsga
''' + ('export STATIC_PRIVSEP_MODEL_DROP_ACK=1\n'if drop else'')+'''echo STATIC_C_MECHANISM_MODEL_ONLY_NO_SOURCE
set +e
/static-privsep-native
rc=$?
echo STATIC_GUEST_EXIT=$rc
sync
poweroff -f
''');os.chmod(root/'init',0o755)
 entries=[('.',stat.S_IFDIR|0o755,b'',0,0)]
 for f in sorted(root.rglob('*')):
  st=f.lstat();rel=str(f.relative_to(root))
  if stat.S_ISDIR(st.st_mode):entries.append((rel,st.st_mode,b'',0,0))
  elif stat.S_ISLNK(st.st_mode):entries.append((rel,st.st_mode,os.readlink(f).encode(),0,0))
  elif stat.S_ISREG(st.st_mode):entries.append((rel,st.st_mode,f.read_bytes(),0,0))
  else:raise RuntimeError('unexpected initramfs object')
 entries += [('dev/console',stat.S_IFCHR|0o600,b'',5,1),('dev/null',stat.S_IFCHR|0o666,b'',1,3)]
 arc=bytearray()
 def add(name,mode,data,ino,major=0,minor=0):
  name=name.encode()+b'\0';fields=[ino,mode,0,0,2 if stat.S_ISDIR(mode)else 1,0,len(data),0,0,major,minor,len(name),0];arc.extend(b'070701'+''.join(f'{x:08x}'for x in fields).encode()+name);arc.extend(b'\0'*((-len(arc))%4));arc.extend(data);arc.extend(b'\0'*((-len(arc))%4))
 for ino,(name,mode,data,ma,mi)in enumerate(entries,1):add(name,mode,data,ino,ma,mi)
 add('TRAILER!!!',0,b'',len(entries)+1);arc.extend(b'\0'*((-len(arc))%512))
 with(case/'initramfs.cpio.gz').open('wb')as f:
  with gzip.GzipFile(fileobj=f,filename='',mode='wb',mtime=0)as z:z.write(arc)
 put(case/'initramfs-manifest.json',{'archive_sha256':sha(case/'initramfs.cpio.gz'),'files':[{'path':n,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}for n,mo,data,ma,mi in entries]})
 put(case/'source-lock.json',{'scope':'ISOLATED_ACTUAL_CPU0_C_MECHANISM_MODEL_NOT_SOURCE','fixture_build':str(build),'fixture_source_lock_sha256':sha(build/'source-lock.json'),'fixture_ELF_sha256':sha(root/'static-privsep-native'),'udevd_sha256':sha(root/'sbin/udevd'),'udevadm_sha256':sha(root/'bin/udevadm'),'provider':str(provider),'provider_source_lock_sha256':sha(provider/'source-lock.json'),'stock_libudev_sha256':sha(root/'lib/libudev.so.1'),'runtime_origin':str(libs),'runtime_link_lock_sha256':sha(libs.parent/'guest-runtime-link-lock.json'),'runtime_original_APK_signature_exit':int((libs.parent/'runtime-apk-verify.exitcode').read_text()),'runtime_files':{f.name:sha(f)for f in(root/'lib').iterdir()if f.is_file()},'busybox_sha256':sha(busy),'kernel_sha256':sha(kernel/'vmlinuz'),'notes_sha256':sha(kernel/'kernel-notes.bin'),'config_sha256':sha(kernel/'kernel.config'),'Source':False,'normal':False,'PG_executed':False,'E':0,'host_network_time_power_actions':0,'QMP_power_commands':0})
def run_case(label,provider,drop=False,old_control=False):
 case=p/label;case.mkdir();make_initramfs(case,provider,drop);name='static-'+label+'-'+meta['nonce'].lower();qargs=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','1024M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-qmp','unix:/proof/qmp.sock,server=on,wait=off','-kernel','/kernel/vmlinuz','-initrd','/proof/initramfs.cpio.gz','-append','console=ttyS0,115200 rdinit=/init panic=-1']
 cmd=['timeout','--signal=TERM','--kill-after=5s','85s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device=/dev/kvm:/dev/kvm','--cpus','2','--memory','3g','--memory-swap','3g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(case)+':/proof:rw','-v',str(kernel)+':/kernel:ro','--entrypoint','/bin/sh',runner,'-c','set +e; /usr/bin/qemu-system-x86_64 "$@"; rc=$?; for m in cpu.stat memory.peak io.stat memory.events; do cat /sys/fs/cgroup/$m >/proof/$m; done; exit "$rc"','static-qemu',*qargs]
 put(case/'command.json',cmd);start=time.monotonic();qs=None;qf=None;actual=None;rows=[]
 with(case/'console.log').open('wb')as log:
  proc=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
  while proc.poll()is None:
   if actual is None:
    try:
     directory=os.open(case,os.O_RDONLY|os.O_DIRECTORY)
     try:qs=socket.socket(socket.AF_UNIX);qs.settimeout(.5);qs.connect('/proc/self/fd/'+str(directory)+'/qmp.sock')
     finally:os.close(directory)
     qf=qs.makefile('rb');json.loads(qf.readline())
     for op in ['qmp_capabilities','query-kvm']:
      qs.sendall((json.dumps({'execute':op})+'\n').encode())
      while True:
       row=json.loads(qf.readline());rows.append(row)
       if'return'in row or'error'in row:break
      assert'error'not in row
     actual=row['return'];assert actual=={'enabled':True,'present':True}
    except(OSError,ValueError):
     if qs:qs.close()
     qs=None;qf=None
   time.sleep(.05)
  rc=proc.wait()
 if qs:qs.close()
 x=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if x.returncode==0:
  a=json.loads(x.stdout)[0];put(case/'container.json',{'ID':a['Id'],'Image':a['Image'],'State':a['State'],'HostConfig':{k:a['HostConfig'].get(k)for k in ['Memory','MemorySwap','NanoCpus','NetworkMode','ReadonlyRootfs','CapDrop','SecurityOpt','PidsLimit']}})
 clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);put(case/'cleanup.json',{'exit':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr});put(case/'qmp.json',rows)
 text=(case/'console.log').read_text(errors='replace');notes=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text);actual_notes=base64.b64decode(''.join(notes[1].split()),validate=True)if notes else None;exits=re.findall(r'^STATIC_GUEST_EXIT=(\d+)\r?$',text,re.M)
 base=rc==0 and actual=={'enabled':True,'present':True}and actual_notes==(kernel/'kernel-notes.bin').read_bytes()and'Kernel panic - not syncing:'not in text
 workers=re.findall(r'STATIC_ORIGINAL_WORKER=(\d+):(\d+) generation=(\d+) seq=(\d+)',text);prod_workers=re.findall(r'fixed IPC original worker (\d+) pidfs (\d+) generation (\d+) seq (\d+)',text);consumed=re.findall(r'STATIC_PRIVSEP_STOCK_LIBUDEV=(\d+)',text);model=re.findall(r'STATIC_MODEL_EMIT=(\d+) kind=3 index=0 action=(\d+)',text);tickets=re.findall(r'STATIC_KERNEL_TICKET=(\d+) action=(\w+)',text);roles=re.findall(r'STATIC_ROLE_ACTUAL=(\S+) pid=(\d+) UID=60004 GID=60004 NNP=1 CAP_ALL=0',text)
 if old_control:
  passed=base and exits==['1']and'STATIC_PRIVSEP_NATIVE_FAIL'in text and'STATIC_ROLE_ACTUAL=main-ready'in text and len(tickets)>=1 and not model and not consumed
  status='EXPECTED_OLD_CPU_PATH_REJECT_OBSERVED'if passed else'OLD_CONTROL_INCONCLUSIVE'
 elif drop:
  passed=base and exits==['0']and text.count('STATIC_PRIVSEP_LOST_ACK_UNKNOWN_NO_RETRY_PASS')==1 and len(workers)==1 and len(model)==1 and len(tickets)==1 and model[0][0]==tickets[0][0] and roles
  status='PASS_LOST_ACK_MODEL_ONLY'if passed else'FAIL_LOST_ACK_OR_FIXTURE'
 else:
  passed=base and exits==['0']and text.count('STATIC_PRIVSEP_NATIVE_PASS_MAIN_WORKER_STOCKLIB_MODEL_NOT_SOURCE')==1 and text.count('STATIC_PRIVSEP_SAME_UID_NONMAIN_REJECTED')==1 and text.count('STATIC_PRIVSEP_MODEL_REPLAY_AND_FAKE_INDEX_REJECTED')==1 and len(workers)==2 and workers[0][:2]==workers[1][:2] and [w[2]for w in workers]==['1','2']and len(prod_workers)==2 and prod_workers[0][:2]==prod_workers[1][:2] and len(consumed)==2 and [m[1]for m in model]==['1','2']and [m[0]for m in model]==consumed and [t[0]for t in tickets]==consumed
  status='PASS_TWO_REAL_CPU_EVENTS_MODEL_ONLY'if passed else'FAIL_MECHANISM_OR_FIXTURE'
 result={'label':label,'status':status,'passed':bool(passed),'seconds':time.monotonic()-start,'QEMU_exit':rc,'Guest_exit':exits,'actual_KVM':actual,'kernel_notes_sha256':hashlib.sha256(actual_notes).hexdigest()if actual_notes else None,'actual_original_workers':workers,'production_worker_trace':prod_workers,'public_libudev_consumed_sequences':consumed,'Root_model_emits':model,'original_kernel_tickets':tickets,'actual_role_status':roles,'old_control_not_release_target':old_control,'Source':False,'normal':False,'E':0,'PG_executed':False,'Root_MODEL_not_real_lease_or_WAL':True,'host_network_time_power_actions':0,'QMP_power_commands':0};put(case/'result.json',result);return result
results=[]
for label,provider,drop,control in [('old-e473-control',old,False,True),('new-two-events',product,False,False),('new-lost-ack',product,True,False)]:
 result=run_case(label,provider,drop,control);results.append(result);put(p/'progress.json',results)
 # 保真实旧拒对照，即使inconclusive也继续获取新机制实际终态，不以old拒替代new通过。
 if not control and not result['passed']:break
final={'component_targets':2,'old_control':results[0],'cases':results,'passed':len(results)==3 and all(x['passed']for x in results),'Source':False,'normal':False,'E':0};put(p/'result.json',final);print(json.dumps(final),flush=True);raise SystemExit(0 if final['passed']else 1)
