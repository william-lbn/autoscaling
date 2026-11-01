from pathlib import Path
import json,subprocess,time,socket,os,hashlib,datetime
p=Path(__file__).parent;name=(p/'container-name.txt').read_text().strip();start=time.monotonic();kvm=None;frames=[];last=None
with(p/'console.log').open('wb')as log:
 proc=subprocess.Popen(json.loads((p/'command.json').read_text()),stdout=log,stderr=subprocess.STDOUT)
 while proc.poll()is None:
  if kvm is None:
   fd=os.open(p,os.O_RDONLY|os.O_DIRECTORY)
   try:
    with socket.socket(socket.AF_UNIX)as s:
     s.settimeout(.5);s.connect('/proc/self/fd/'+str(fd)+'/qmp.sock');f=s.makefile('rb');frames.append(json.loads(f.readline()))
     for seq,op in enumerate(['qmp_capabilities','query-kvm'],1):
      s.sendall((json.dumps({'execute':op,'id':seq})+'\n').encode())
      while True:
       x=json.loads(f.readline());frames.append(x)
       if x.get('id')==seq:break
      assert 'error'not in x,x
      if op=='query-kvm':kvm=x['return'];assert kvm['enabled']and kvm['present']
   except(OSError,ValueError,AssertionError)as e:last=repr(e)
   finally:os.close(fd)
  time.sleep(.05)
 rc=proc.wait()
(p/'qmp-frames.json').write_text(json.dumps(frames,indent=2)+'\n')
inspect=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
if inspect.returncode==0:
 x=json.loads(inspect.stdout)[0];(p/'container-state.json').write_text(json.dumps({'Id':x['Id'],'Image':x['Image'],'State':x['State'],'HostConfig':{k:x['HostConfig'].get(k)for k in ['Memory','MemorySwap','NanoCpus','PidsLimit','NetworkMode','CapDrop','SecurityOpt']}},indent=2)+'\n')
rm=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True);(p/'cleanup.json').write_text(json.dumps({'exit':rm.returncode,'stdout':rm.stdout,'stderr':rm.stderr},indent=2)+'\n')
text=(p/'console.log').read_text(errors='replace');required=['ORDINARY_SOCKET_MECHANISM_GUEST_EXIT=0','--- PASS: TestNativeOrdinaryNetlinkSocketFacts','NORMAL_UDEV_SOCKET_FACT_JSON','NORMAL_UDEV_SOCKET_OPEN_JSON'];passed=rc==0 and kvm is not None and all(s in text for s in required) and 'Kernel panic - not syncing:'not in text and 'BUG:'not in text
result={'status':'PASS_MECHANISM_ONLY'if passed else'FAIL_MECHANISM','host_QEMU_exit':rc,'QMP_KVM':kvm,'last_QMP_retry':last,'seconds':time.monotonic()-start,'console_sha256':hashlib.sha256((p/'console.log').read_bytes()).hexdigest(),'Guest_roots':1,'actual_dedicated_child':0,'fullSource':False,'normal':False,'PG_started':False,'E':0,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()};(p/'result.json').write_text(json.dumps(result,indent=2)+'\n');raise SystemExit(0 if passed else 1)
