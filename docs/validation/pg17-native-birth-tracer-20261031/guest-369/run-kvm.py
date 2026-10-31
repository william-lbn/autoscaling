from pathlib import Path
import json,hashlib,subprocess,socket,time,datetime,re,base64
out=Path('/data/neon-build/validation/pg17-go-native-tracer-20261030T235629Z/guest-369-attempt-02')
run=out/'run';run.mkdir(exist_ok=True)
kernel=Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export')
lock=json.loads((out/'runtime-dependency-lock.json').read_text())
for f,h in lock['kernel']['sha256'].items():assert hashlib.sha256((kernel/f).read_bytes()).hexdigest()==h
assert hashlib.sha256((out/'root/probe/tracer.test').read_bytes()).hexdigest()==lock['staticTestBinarySHA256']
image='127.0.0.1:5001/neon/neonvm-runner@sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e'
proof=subprocess.run(['docker','run','--rm','--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--entrypoint','sha256sum',image,'/usr/bin/qemu-system-x86_64'],capture_output=True,text=True)
(out/'qemu-proof.txt').write_text(proof.stdout+proof.stderr)
assert proof.returncode==0 and proof.stdout.split()[0]=='8d8d8ee83471749d0b882ce34344638e0ac0fa2390cd822b98d9b6314e2f9cb5'
name='neon-pg17-tracer-369-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ').lower()
cmdline='console=ttyS0,115200 earlycon=uart8250,io,0x3f8,115200n8 rdinit=/init panic=-1 kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf neonvm.pid_max_limit=512 pg17_native_tracer_component=1'
qemu=['-machine','q35','-accel','kvm','-cpu','host','-smp','2,maxcpus=2','-m','1024M','-nographic','-monitor','none','-no-reboot','-no-user-config','-nic','none','-S','-qmp','unix:/qmp/control.sock,server=on,wait=off','-kernel','/kernel/vmlinuz','-initrd','/probe/initramfs.cpio.gz','-append',cmdline]
docker=['docker','run','--name',name,'--pull=never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--device','/dev/kvm:/dev/kvm','--cpus','2','--memory','2g','--memory-swap','2g','--pids-limit','128','--ulimit','core=0','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16m','-v',str(out)+':/probe:ro','-v',str(kernel)+':/kernel:ro','-v',str(run)+':/qmp:rw','--entrypoint','/usr/bin/qemu-system-x86_64',image,*qemu]
cmd=['timeout','--signal=TERM','--kill-after=10s','180s',*docker]
(out/'kvm-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
messages=[];errors=[];rc=None;kvm=None;start=datetime.datetime.now(datetime.timezone.utc).isoformat();tick=time.monotonic()
try:
 with (out/'console.log').open('wb') as log:
  process=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
  client=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);client.settimeout(5)
  until=time.monotonic()+15
  while True:
   try:client.connect(str(run/'control.sock'));break
   except (FileNotFoundError,ConnectionRefusedError):
    if process.poll() is not None or time.monotonic()>until:raise
    time.sleep(.05)
  with client,client.makefile('rwb',buffering=0) as stream:
   greeting=json.loads(stream.readline());messages.append({'receive':greeting});assert 'QMP' in greeting
   def execute(command,ident):
    request={'execute':command,'id':ident};messages.append({'send':request});stream.write((json.dumps(request)+'\r\n').encode())
    while True:
     row=json.loads(stream.readline());messages.append({'receive':row})
     if row.get('id')==ident:assert 'error' not in row,row;return row['return']
   execute('qmp_capabilities','capabilities')
   kvm=execute('query-kvm','actual-kvm');assert kvm=={'enabled':True,'present':True},kvm
   status=execute('query-status','before-cont');assert status['status'] in ['prelaunch','paused'],status
   execute('cont','run-Guest')
  rc=process.wait(timeout=195)
except Exception as e:
 errors.append(type(e).__name__+': '+str(e))
 subprocess.run(['docker','kill',name],capture_output=True,text=True)
 if 'process' in locals():
  try:rc=process.wait(timeout=15)
  except Exception as e:errors.append('owned cleanup: '+str(e))
finally:
 (out/'qmp-transcript.json').write_text(json.dumps(messages,indent=2)+'\n')
 ins=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if ins.returncode==0:
  v=json.loads(ins.stdout)[0];(out/'container-state.json').write_text(json.dumps({'Id':v['Id'],'Image':v['Image'],'State':v['State'],'NetworkMode':v['HostConfig']['NetworkMode'],'Devices':v['HostConfig']['Devices']},indent=2)+'\n')
 cleanup=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True)
 (out/'cleanup.json').write_text(json.dumps({'exit':cleanup.returncode,'stdout':cleanup.stdout,'stderr':cleanup.stderr},indent=2)+'\n')
text=(out/'console.log').read_text(errors='replace')
releases=re.findall(r'^PG17_TRACER_RELEASE=([^\r\n]+)',text,re.M)
notes=re.findall(r'^PG17_TRACER_NOTES_SHA256=([^\r\n]+)',text,re.M)
hardcap=re.findall(r'^PG17_TRACER_HARDCAP=(\d+)',text,re.M)
binary=re.findall(r'^PG17_TRACER_BINARY_SHA256=([^\r\n]+)',text,re.M)
guest_rc=[int(x) for x in re.findall(r'^PG17_TRACER_GUEST_EXIT=(\d+)\r?$',text,re.M)]
reports={};(out/'guest-reports').mkdir(exist_ok=True)
for n,h,b in re.findall(r'^PG17_TRACER_FILE=([^:\r\n]+):([0-9a-f]{64}):([A-Za-z0-9+/=]+)\r?$',text,re.M):
 try:
  assert n==Path(n).name and n not in reports
  raw=base64.b64decode(b,validate=True);assert hashlib.sha256(raw).hexdigest()==h
  reports[n]=json.loads(raw);(out/'guest-reports'/n).write_bytes(raw)
 except Exception as e:errors.append('Guest report '+n+': '+str(e))
roots=re.findall(r'^--- PASS: (TestNativeBirthTracerKernel\S+) \(',text,re.M)
subcases=re.findall(r'^    --- PASS: (TestNativeBirthTracerKernel\S+) \(',text,re.M)
passed=not errors and rc==0 and cleanup.returncode==0 and kvm=={'enabled':True,'present':True} and releases==[lock['kernel']['release']] and notes==[lock['kernel']['sha256']['kernel-notes.bin']] and hardcap==['512'] and binary==[lock['staticTestBinarySHA256']] and guest_rc==[0] and len(roots)==3 and len(subcases)==13 and len(reports)==14
if passed:
 passed=reports['session-valid.json']['transcript']['DetachedNonAuthorizing'] and all(not r['transcript']['DetachedNonAuthorizing'] for n,r in reports.items() if n.startswith('session-') and n!='session-valid.json') and all(reports['retirement-confirmed-'+x+'.json']['transcript']['TracerThreadRetired'] for x in ['true','false']) and reports['pidfd-reaper.json']['forcedReuseGuestOnly'] and reports['pidfd-reaper.json']['actualNumericReuse']
result={'id':['PG17-S-TRACER-001','PG17-S-TRACER-002','PG17-S-TRACER-003'],'passed':bool(passed),'status':'PASS_COMPONENT_ONLY' if passed else 'FAIL_COMPONENT_OR_ENV','scope':'accurate369 Guest Go+PG wire/ptrace only; synthetic Source/fixture WAL, no production Source/normal/K8s','startUTC':start,'endUTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'seconds':time.monotonic()-tick,'hostQemuExitCode':rc,'actualGuestExitCodes':guest_rc,'queryKVM':kvm,'TCGFallback':False,'kernelReleaseObserved':releases,'kernelNotesObserved':notes,'earlyPIDHardcapObserved':hardcap,'sameHostAndGuestTestBinarySHA256':binary,'GoRootsPassed':roots,'GoNamedSubcasesPassed':len(subcases),'GuestReportsVerified':len(reports),'kernelSourceLock':lock['kernel'],'initramfsSHA256':hashlib.sha256((out/'initramfs.cpio.gz').read_bytes()).hexdigest(),'consoleSHA256':hashlib.sha256((out/'console.log').read_bytes()).hexdigest(),'errors':errors,'normalDecreaseExecuted':False,'productionWALSourceIntegrated':False,'VMShrinkPermit':False}
(out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2),flush=True)
if not passed:print('\n'.join(text.splitlines()[-110:]),flush=True)
raise SystemExit(0 if passed else 1)
