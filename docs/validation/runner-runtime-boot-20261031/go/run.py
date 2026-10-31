from pathlib import Path
import subprocess,json,time,os
r=Path(__file__).resolve().parent
cmd=['docker','buildx','build','--builder','neon-data','--network','host','--allow','network.host','--progress','plain','--build-arg','TEST_NONCE='+r.name,'--metadata-file',str(r/'build-metadata.json'),'--output','type=local,dest='+str(r/'receipt'),'-f',str(r/'Dockerfile'),str(r)]
(r/'command.json').write_text(json.dumps({'argv':cmd,'actual_test_network':'none','gomaxprocs':2,'gomemlimit':'1GiB per Go process','client_cgroup':'3CPU/4GiB; BuildKit worker retains its own shared service limits, not covered by client limits'},indent=2)+'\n')
start=time.monotonic()
with(r/'run.log').open('w')as log:out=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
result={'build_exit':out.returncode,'seconds':time.monotonic()-start}
for name in ['test.rc','source-check.rc']:
 f=r/'receipt'/name
 if f.exists():result[name]=int(f.read_text().strip())
result['pass']=out.returncode==0 and result.get('test.rc')==0 and result.get('source-check.rc')==0
(r/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(0 if result['pass']else 1)
