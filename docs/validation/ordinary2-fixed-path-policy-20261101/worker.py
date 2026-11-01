from pathlib import Path
import os,subprocess,json,datetime,time
p=Path(__file__).parent;start=time.monotonic()
raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout
env=os.environ.copy();env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in[x.split(b'=',1)]})
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=3g','--resource','memory-swap=3g','--resource','cpu-quota=300000','--network','none','--progress','plain','--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
with(p/'run.log').open('wb')as log:r=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
result={'build_exit':r.returncode,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'seconds':time.monotonic()-start,'normal':False,'Source':False,'E':0}
names=['u','cli','darwin']
for name in names:
 f=p/'export'/(name+'.exitcode');result[name+'_exit']=int(f.read_text())if f.exists()else None
events=[]
if(p/'export/test.json').exists():events=[json.loads(l)for l in(p/'export/test.json').read_text().splitlines()if l.strip()]
passed=[e['Test']for e in events if e.get('Action')=='pass'and e.get('Test')]
expected=json.loads((p/'source-lock.json').read_text())['expected_tests']
result.update({'passed_roots':sorted([t for t in passed if'/'not in t]),'passed_subtests':len([t for t in passed if'/'in t]),'missing_expected':sorted(set(expected)-set(passed)),'skipped':[e.get('Test','PACKAGE')for e in events if e.get('Action')=='skip']})
(p/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(0 if r.returncode==0 and all(result[n+'_exit']==0 for n in names)and not result['missing_expected']and not result['skipped']else 1)
