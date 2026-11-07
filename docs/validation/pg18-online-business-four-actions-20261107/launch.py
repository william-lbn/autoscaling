from pathlib import Path as P
import subprocess as s,os,json,datetime,time
os.umask(0o077);d=P(__file__).parent
s.run(['python3','-u',str(d/'prepare-baseline.py')],check=True,timeout=220)
with(d/'business.log').open('xb')as f:
 r=s.run(['python3','-u',str(d/'app_proxy.py'),'--parameters',str(d/'parameters.json'),'--publisher-receipt','/data/neon-k8s-pg18/private/PG18-正式Proxy路由-940b25fb/publisher.stdout','--mode','app-wal'],stdout=f,stderr=f,timeout=2300)
(d/'business-exit.PUBLIC.json').write_text(json.dumps({'exit':r.returncode,'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}))
assert r.returncode==0,'PG18 ordinary business four-action validation failed'
