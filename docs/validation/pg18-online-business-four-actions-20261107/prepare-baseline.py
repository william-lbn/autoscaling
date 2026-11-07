from pathlib import Path as P
import json,subprocess as s,base64,zlib,time,hashlib,os
os.umask(0o077);d=P(__file__).parent
deadline=time.monotonic()+180
while True:
 vm=json.loads(s.check_output(['kubectl','--kubeconfig=/root/.kube/config','-n','neon','get','virtualmachines.vm.neon.tech','cp-24c41e23bb104ead','-o','json'],timeout=15))
 e=json.loads(vm['metadata']['annotations']['internal.autoscaling.neon.tech/dsga-journal-v1']);raw=zlib.decompress(base64.b64decode(e['data']));assert hashlib.sha256(raw).hexdigest()==e['sha256'];m=json.loads(raw)
 assert vm['metadata']['uid']=='02679a05-092f-4a50-b4f2-391fae7ac63e' and not m.get('service_closure') and not m['active'] and m['high_water']is None and not m['history']
 held=m.get('initial_held')
 if held and held.get('admission',{}).get('phase')=='settled':
  assert held['origin']==m['owner'] and m['pg_instance']['pg_boot_id']=='15917482883749979035'
  budget=json.loads(base64.b64decode(held['budget']))
  assert hashlib.sha256(base64.b64decode(held['budget'])).hexdigest()==held['budget_sha256']
  assert budget['vm_memory_bytes']=='7516192768' and budget['pg_aggregate']['bound']['upper_bytes']=='1077411840' and vm['status']['memorySize']=='7Gi'
  n=d/'Native初始实读'
  with(n/'unit.log').open('xb')as f:q=s.run(['python3',str(n/'native-read.py')],stdout=f,stderr=f,timeout=100)
  assert q.returncode==0
  pg=json.loads((n/'memory-response.private.json').read_text())['observed']
  assert pg['admission_open'] and pg['high_buffers']==pg['low_buffers']==256 and pg['capacity_generation']=='1' and pg['instance']==m['pg_instance'] and pg['issuer_floor_epoch']==m['owner']['epoch']
  (d/'settled-baseline.private.json').write_text(json.dumps(m))
  (d/'本实例起始实读.PUBLIC.json').write_text(json.dumps({'uid':vm['metadata']['uid'],'original_key':m['high_water'],'owner_epoch':m['owner']['epoch'],'PG_boot':pg['instance']['pg_boot_id'],'capacity_generation':pg['capacity_generation'],'normal_memory_max_after':'1077411840','held_source':'initial_held','held_sha256':hashlib.sha256(json.dumps(held,sort_keys=True,separators=(',',':')).encode()).hexdigest()},indent=2))
  break
 if time.monotonic()>deadline:raise RuntimeError('initial admission not settled')
 time.sleep(4)
