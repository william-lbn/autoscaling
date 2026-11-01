from pathlib import Path
import os,sys,json,hashlib,shutil,subprocess,time
p=Path(__file__).parent
compiled=Path('/data/neon-build/validation/normal-inflight-metadata-origin-native-build-20261101T203708Z-6ff97d')
reused=compiled
build=p/'prepared';(build/'export').mkdir(parents=True)
for n in ['service.test','host-memory.test','resource-guard.real']:
 artifact=(compiled if n=='service.test' else reused)/'export'/n; assert artifact.stat().st_mode&0o777==0o755; os.link(artifact,build/'export'/n)
shutil.copytree(compiled/'export/fixture',build/'export/fixture',symlinks=True)
shutil.copyfile(compiled/'source-lock.json',build/'source-lock.json')
root=build/'export/fixture/root'
provider=Path('/data/neon-build/validation/normal-stock-udev-assets-go-20261101T130302Z-fa4381/provider-fixture')
with(p/'merge-provider.log').open('wb')as f:
 r=subprocess.run(['/usr/bin/python3',str(p/'merge-provider.py'),str(root),str(provider),str(p/'merge-proof')],stdout=f,stderr=subprocess.STDOUT)
(p/'merge-provider.exitcode').write_text(str(r.returncode)+'\n')
if r.returncode:raise SystemExit(r.returncode)
m=root/'neonvm/config/dsga-normal-system-source.json';raw=m.read_bytes();base=json.loads(raw)['declared_image_sha256'];(p/'seed-system-source.json').write_bytes(raw)
plans=[]
for f in(root/'neonvm/config').glob('*.json'):
 try:j=json.loads(f.read_bytes())
 except(ValueError,UnicodeError):continue
 if isinstance(j,dict)and'compute_argv'in j:plans.append(f)
assert len(plans)==1,plans
plan='/proof/fixed-build-plan.json';(p/'fixed-build-plan.json').write_bytes(plans[0].read_bytes());m.unlink()
# 新disposable final build中只移除已知seed的3资产，保留原raw作为来源记录；不复用上轮partial。
for name in ['dsga-normal-build-plan.json','dsga-normal-role-broker.json','dsga-kernel-control-source.json']:
 f=root/'neonvm/config'/name
 (p/('seed-'+name)).write_bytes(f.read_bytes());f.unlink()
builder=reused/'export/vm-builder.real'
tools='127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1'
cmd=['docker','run','--rm','--pull=never','--network=none','--cpus=2','--memory=3g','--memory-swap=3g','-v',str(build/'export')+':/out:rw','-v',str(builder)+':/builder:ro','-v',str(p/'merge-proof')+':/merge-proof:ro','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set +e; /builder --system-mode=normal-split-v3 --normal-manifest-root=/out/fixture/root --normal-build-plan='+plan+' --normal-base-image-sha='+base+' --normal-ordinary-controls-version=2 --normal-ordinary-package-lock=/merge-proof/package-lock-derived.json >/out/builder-final.log 2>&1; rc=$?; echo $rc >/out/builder-final.exitcode; if test $rc = 0; then DSGA_SYSTEM_LIFECYCLE_EXPORT=/out/fixture /out/service.test -test.run=^TestFinalizeNormalStockUdevPreparationFixture$ -test.v -test.timeout=30s >/out/fixture-final.log 2>&1; rc=$?; echo $rc >/out/fixture-final.exitcode; fi; cat /sys/fs/cgroup/cpu.stat >/out/fixture.cpu.stat; cat /sys/fs/cgroup/memory.peak >/out/fixture.memory.peak; cat /sys/fs/cgroup/io.stat >/out/fixture.io.stat; exit $rc']
(p/'fixture-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with(p/'fixture-command.log').open('wb')as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
(p/'fixture-command.exitcode').write_text(str(r.returncode)+'\n')
if r.returncode:raise SystemExit(r.returncode)
(p/'compiled-artifact-link.json').write_text(json.dumps({'compiled_source':str(compiled),'compiled_source_lock_sha256':hashlib.sha256((compiled/'source-lock.json').read_bytes()).hexdigest(),'Guest_ELF_sha256':hashlib.sha256((build/'export/service.test').read_bytes()).hexdigest(),'builder_sha256':hashlib.sha256(builder.read_bytes()).hexdigest(),'scope':'new disposable provider fixture; same exact compiled Go image; no normal permission'},indent=2)+'\n')

from pathlib import Path
import os,sys,stat,gzip,json,hashlib,shutil,subprocess,time,socket,re,base64
p=Path(__file__).parent
build=p/'prepared';root=p/'rootdisk'
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
assert (root/'etc/passwd').is_file()
assert (root/'etc/group').is_file()
step('mkfs',['docker','run','--rm','--pull=never','--network=none','--cpus=1','--memory=1g','--memory-swap=1g','-v',str(root)+':/rootdisk:ro','-v',str(p)+':/proof:rw','--entrypoint','/bin/bash',tools,'-c','set -eu; truncate -s 512M /proof/root.ext4; mkfs.ext4 -q -F -L fs-origin-component -d /rootdisk /proof/root.ext4'])
step('iso',['docker','run','--rm','--network=none','--pull=never','--cpus=1','--memory=512m','--memory-swap=512m','-v',str(build/'export/fixture/isofiles')+':/isofiles:ro','-v',str(p)+':/proof:rw','--entrypoint','genisoimage','neon-test-only/dsga-kernel-bootstrap-iso:20261031','-quiet','-R','-J','-o','/proof/runtime.iso','/isofiles'])
init=p/'initramfs-root';init.mkdir()
for d in ['bin','lib','dev','proc','sys','newroot']:(init/d).mkdir()
old=Path('/data/neon-build/validation/pg17-s-kernel-001-20261030T154132Z/initramfs-root')
for n in ['bin/busybox','lib/ld-musl-x86_64.so.1']:shutil.copyfile(old/n,init/n);os.chmod(init/n,0o755)
os.symlink('ld-musl-x86_64.so.1',init/'lib/libc.musl-x86_64.so.1')
(init/'init').write_text("""#!/bin/busybox sh
set -eu
export PATH=/bin
export DSGA_ROLE_TRANSFER2_COMPONENT=DEV18C_SINGLE8_SAMEGUARD_NOT_NORMAL
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
kernel=Path('/data/neon-build/validation/normal-kernel-metadata-build-20261101T023713Z-58603b/build/export')
assert sha(kernel/'vmlinuz').startswith('18c9fd')
required=(build/'export/fixture/required-kernel-args.txt').read_text().strip()

# Host生产lifecycle负责真实QMP/Unixpeer/pidfd和原唯一Wait；外层不另连其单QMP monitor。
name=p.name.lower()
command=['timeout','--signal=TERM','--kill-after=10s','250s','docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--cap-add','SYS_PTRACE','--cap-add','KILL','--cap-add','SETUID','--cap-add','SETGID','--cap-add','DAC_OVERRIDE','--user','0:0','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','3g','--memory-swap','3g','--pids-limit','128','--tmpfs','/tmp:rw,nosuid,nodev,size=128m,mode=1777','-e','GOMAXPROCS=2','-e','DSGA_INITIAL_HOST_NATIVE=qemu18c-transfer2','-e','DSGA_INITIAL_HOST_OUTPUT=/proof','-v',str(p)+':/proof:rw','-v',str(build/'export/host-memory.test')+':/native-host.test:ro','-v',str(kernel)+':/kernel:ro','--entrypoint','/native-host.test',runner,'-test.run=^TestNativeNormalInflightMetadataOriginHost$','-test.v','-test.count=1','-test.timeout=235s']
put('native-command.json',command)
put('source-lock.json',{'build':str(build),'source_lock_sha256':sha(build/'source-lock.json'),'Host_ELF_sha256':sha(build/'export/host-memory.test'),'Guest_ELF_sha256':sha(build/'export/service.test'),'vminit_sha256':sha(root/'neonvm/bin/vminit'),'System_manifest_sha256':sha(root/'neonvm/config/dsga-normal-system-source.json'),'runtime_ISO_sha256':sha(p/'runtime.iso'),'root_ext4_before_sha256':sha(p/'root.ext4'),'kernel_image_sha256':sha(kernel/'vmlinuz'),'kernel_config_sha256':sha(kernel/'kernel.config'),'kernel_notes_sha256':sha(kernel/'kernel-notes.bin'),'scope':'ACTUAL_SINGLE8_THEN_FS2_COMPONENT_HOST_API_MODEL_SYSTEM_STANDINS_NOT_PG_NORMAL','normal_authorized':False})
start=time.monotonic()
with(p/'native-host.log').open('wb')as log:
 proc=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
 resources=None
 while proc.poll()is None:
  if resources is None:
   check=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
   if check.returncode==0:
    obj=json.loads(check.stdout)[0];pid=obj['State'].get('Pid',0)
    if pid>0:
     try:
      cg=Path('/proc/'+str(pid)+'/cgroup').read_text();relative=next(x[3:]for x in cg.splitlines()if x.startswith('0::'))
      cpath=Path('/sys/fs/cgroup')/relative.lstrip('/')
      knobs={key:(cpath/key).read_text().strip()for key in ['memory.max','memory.swap.max','cpu.max','pids.max']}
      resources={'Docker_ID':obj['Id'],'Host_PID':pid,'Host_cgroup':relative,'knobs':knobs,'Memory':obj['HostConfig']['Memory'],'MemorySwap':obj['HostConfig']['MemorySwap'],'NanoCpus':obj['HostConfig']['NanoCpus'],'NetworkMode':obj['HostConfig']['NetworkMode'],'ReadonlyRootfs':obj['HostConfig']['ReadonlyRootfs']};put('actual-runtime-resources.json',resources)
     except(OSError,StopIteration):pass
  
  if resources is not None:
   try:
    usage={key:(cpath/key).read_text().strip()for key in ['cpu.stat','memory.peak','io.stat','memory.events']}
    put('actual-usage-last.json',usage)
   except OSError:pass
  time.sleep(.05)
 rc=proc.wait()
inspection=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
if inspection.returncode==0:
 obj=json.loads(inspection.stdout)[0];put('actual-container-terminal.json',{'ID':obj['Id'],'Image':obj['Image'],'State':obj['State'],'HostConfig':{key:obj['HostConfig'][key]for key in ['Memory','MemorySwap','NanoCpus','PidsLimit','NetworkMode','ReadonlyRootfs']}})

clean=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);put('cleanup.json',{'exit':clean.returncode,'stdout':clean.stdout,'stderr':clean.stderr})
text=(p/'console.log').read_text(errors='replace')if(p/'console.log').exists()else''
notes=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text)
actualNotes=base64.b64decode(''.join(notes[1].split()),validate=True)if notes else None


exits=re.findall(r'^STOCK_PREPARATION_GUEST_EXIT=(\d+)\r?$',text,re.M)
initial=re.findall(r'^STOCK_PREPARATION_INITIAL_EXIT=(\d+)\r?$',text,re.M)
alive=re.findall(r'^STOCK_PREPARATION_R1_ALIVE_REFERENCE_SHA256 ([0-9a-f]{64})\r?$',text,re.M)
exited=re.findall(r'^STOCK_PREPARATION_R1_EXITED_REFERENCE_SHA256 ([0-9a-f]{64})\r?$',text,re.M)
records=[json.loads(x)for x in re.findall(r'^STOCK_PREPARATION_ORIGINAL_JSON (\{[^\r\n]+\})\r?$',text,re.M)]
hostlog=(p/'native-host.log').read_text(errors='replace')
terminal=json.loads((p/'terminal.json').read_text())if(p/'terminal.json').exists()else None
kvm=json.loads((p/'actual-runas-peer.json').read_text()).get('actual_query_kvm')if(p/'actual-runas-peer.json').exists()else None
resources=json.loads((p/'actual-runtime-resources.json').read_text())if(p/'actual-runtime-resources.json').exists()else None
resource_ok=resources is not None and resources['knobs']=={'memory.max':str(3<<30),'memory.swap.max':'0','cpu.max':'200000 100000','pids.max':'128'} and resources['NetworkMode']=='none' and resources['ReadonlyRootfs']
passed=resource_ok and rc==0 and exits==['0'] and initial==['0'] and len(alive)==1 and alive==exited and len(records)==1 and records[0]['targets']>0 and records[0]['typed_source'] and actualNotes==(kernel/'kernel-notes.bin').read_bytes() and '--- PASS: TestNativeNormalInflightMetadataOriginHost'in hostlog and terminal is not None and terminal['actual_unique_Wait_exit']==0 and kvm=={'enabled':True,'present':True} and 'Kernel panic - not syncing:'not in text
result={'stable_id':'NEON-S-INFLIGHT-METADATA-ORIGIN-001','status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT_OR_FIXTURE','Host_container_exit':rc,'Guest_exits':exits,'initial_exits':initial,'R1_alive_refs':alive,'R1_exited_refs':exited,'actual_stock_receipts':records,'actual_kvm':kvm,'seconds':time.monotonic()-start,'actual_notes_sha256':hashlib.sha256(actualNotes).hexdigest()if actualNotes else None,'Guest_ELF_sha256':sha(build/'export/service.test'),'Host_ELF_sha256':sha(build/'export/host-memory.test'),'Host_API_is_model':True,'production_CLI_only_compiled':True,'ordinary_services_executed':False,'PG_executed':False,'normal_authorized':False,'E':0}
modules=[json.loads(x)for x in re.findall(r'^INFLIGHT_METADATA_ORIGIN_JSON (\{[^\r\n]+\})\r?$',text,re.M)]
module_exits=re.findall(r'^INFLIGHT_METADATA_GUEST_EXIT=(\d+)\r?$',text,re.M)
module_ok=len(modules)==1 and module_exits==['0'] and modules[0]['reference']['maximum_bytes']==4<<30 and modules[0]['reference']['compiled_cpu_ceiling']==64 and modules[0]['budget']['candidate_fits'] is False and modules[0]['normal_authorized'] is False and modules[0]['Host_capacity_guaranteed'] is False
passed=passed and module_ok
result.update({'status':'PASS_COMPONENT_ONLY'if passed else'FAIL_COMPONENT_OR_FIXTURE','actual_module_records':modules,'module_exits':module_exits,'module_original_R2_origin_verified':module_ok,'full_Source_qualified':False,'Host_maximum_capacity_guaranteed':False,'old_stock_S_is_prerequisite_not_additional_target':True})
put('result.json',result);print(json.dumps(result),flush=True);raise SystemExit(int(not passed))
