from pathlib import Path
import json,subprocess,time,socket
d=Path(__file__).parent;t=time.monotonic();cmd=json.loads((d/'command.json').read_text())
with (d/'console.log').open('wb') as f:
 p=subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT)
 error=None;frames=[]
 try:
  while not (d/'qmp.sock').exists():
   if p.poll() is not None:raise RuntimeError('QEMU exited before QMP')
   if time.monotonic()-t>15:raise TimeoutError('QMP socket missing')
   time.sleep(.05)
  sock=socket.socket(socket.AF_UNIX);sock.settimeout(3);sock.connect(str(d/'qmp.sock'))
  io=sock.makefile('rwb',buffering=0)
  def receive():
   raw=io.readline();frames.append(raw.decode());return json.loads(raw)
  greeting=receive();assert 'QMP' in greeting
  for seq,command in enumerate(['qmp_capabilities','query-kvm','query-version'],1):
   io.write((json.dumps({'execute':command,'id':seq})+'\n').encode())
   while True:
    v=receive()
    if v.get('id')==seq:break
   if 'error' in v:raise RuntimeError('QMP command failed')
   if command=='query-kvm':assert v['return']=={'enabled':True,'present':True}
  sock.close()
  rc=p.wait(timeout=180)
 except Exception as e:
  error=str(e)
  subprocess.run(['docker','kill',cmd[3]],capture_output=True)
  rc=p.wait(timeout=15)
(d/'qmp-frames.json').write_text(json.dumps(frames,indent=2)+'\n')
state=subprocess.run(['docker','inspect','--format','{{json .State}}',cmd[3]],capture_output=True,text=True)
(d/'container-state.json').write_text(state.stdout)
console=(d/'console.log').read_text()
passed=rc==0 and error is None and console.count('KERNEL_CONTROL_GUEST_EXIT=0')==1 and console.count('KERNEL_CONTROL_COMPONENT_ACTUAL9_ROOT_UNFILTERED=1')==1
(d/'result.json').write_text(json.dumps({'pass':passed,'docker_qemu_exit':rc,'driver_error':error,'seconds':time.monotonic()-t,'Guest_exit0_count':console.count('KERNEL_CONTROL_GUEST_EXIT=0'),'actual9_root_unfiltered_marker_count':console.count('KERNEL_CONTROL_COMPONENT_ACTUAL9_ROOT_UNFILTERED=1'),'normal_authorized':False,'E':0},indent=2)+'\n')
raise SystemExit(0 if passed else 1)
