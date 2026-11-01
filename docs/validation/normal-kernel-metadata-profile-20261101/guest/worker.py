from pathlib import Path
import json,subprocess,time,socket,re,datetime
d=Path(__file__).parent;t=time.monotonic();cmd=json.loads((d/'command.json').read_text());inputs=json.loads((d/'inputs.json').read_text());frames=[];error=None
with(d/'console.log').open('wb')as f:
 p=subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT)
 try:
  while not(d/'qmp.sock').exists():
   if p.poll()is not None:raise RuntimeError('QEMU exited before QMP')
   if time.monotonic()-t>15:raise TimeoutError('QMP socket missing')
   time.sleep(.05)
  sock=socket.socket(socket.AF_UNIX);sock.settimeout(3);sock.connect(str(d/'qmp.sock'));stream=sock.makefile('rwb',buffering=0)
  def receive():
   raw=stream.readline();frames.append(raw.decode());return json.loads(raw)
  assert 'QMP'in receive()
  for seq,command in enumerate(['qmp_capabilities','query-kvm','query-version'],1):
   stream.write((json.dumps({'execute':command,'id':seq})+chr(10)).encode())
   while True:
    item=receive()
    if item.get('id')==seq:break
   assert'error'not in item
   if command=='query-kvm':assert item['return']=={'enabled':True,'present':True}
  sock.close();rc=p.wait(timeout=150)
 except Exception as exc:
  error=repr(exc);subprocess.run(['docker','kill',cmd[3]],capture_output=True);rc=p.wait(timeout=15)
(d/'qmp-frames.json').write_text(json.dumps(frames,indent=2)+chr(10));state=subprocess.run(['docker','inspect','--format','{{json .State}}',cmd[3]],capture_output=True,text=True);(d/'container-state.json').write_text(state.stdout)
text=(d/'console.log').read_text(errors='replace');reports=re.findall(r'^METADATA_CHILD_RESULT=(\{[^\r\n]+\})\r?$',text,re.M);child=json.loads(reports[0])if len(reports)==1 else None
if child:(d/'child-report.json').write_text(json.dumps(child,indent=2)+chr(10))
child_status=text.split('METADATA_ACTOR=dedicated-test-child-uid0-cap0',1)[1].split('METADATA_CHILD_RESULT=',1)[0]if'METADATA_ACTOR=dedicated-test-child-uid0-cap0'in text else''
caps_ok=all(re.search('^'+k+r':\s+0000000000000000\r?$',child_status,re.M)for k in ['CapInh','CapPrm','CapEff','CapBnd','CapAmb'])
NNP_ok=bool(re.search(r'^NoNewPrivs:\s+1\r?$',child_status,re.M));uid_ok=bool(re.search(r'^Uid:\s+0\s+0\s+0\s+0\r?$',child_status,re.M))
passed=rc==0 and error is None and text.count('CONTROL_METADATA_GUEST_EXIT=0')==1 and text.count('METADATA_COMPONENT_PASS=1')==1 and text.count('METADATA_FIXTURE_EMPTY=1')==1 and len(reports)==1 and child['denied']==16 and child['ordinary_positive']==18 and child['fixture_unchanged']and caps_ok and NNP_ok and uid_ok and 'METADATA_KERNEL_RELEASE='+inputs['kernel_tuple']['release']in text and 'METADATA_KERNEL_NOTES='+inputs['kernel_tuple']['notes_sha256']in text
receipt={'pass':bool(passed),'docker_QEMU_exit':rc,'driver_error':error,'seconds':time.monotonic()-t,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'Guest_exit0_count':text.count('CONTROL_METADATA_GUEST_EXIT=0'),'dedicated_empty_fixture_count':text.count('METADATA_FIXTURE_EMPTY=1'),'child_report':child,'actual_child_uid0_caps5zero_NNP1':bool(caps_ok and NNP_ok and uid_ok),'normal_authorized':False,'E':0};(d/'result.json').write_text(json.dumps(receipt,indent=2)+chr(10));raise SystemExit(0 if passed else 1)
