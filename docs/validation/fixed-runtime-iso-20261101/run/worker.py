from pathlib import Path
import subprocess,json,sys,time
u=Path(sys.argv[1]);nonce=sys.argv[2];a=['docker','buildx','build','--builder','neon-data','--resource','memory=2g','--resource','memory-swap=2g','--resource','cpu-quota=200000','--progress','plain','--build-arg','NONCE='+nonce,'--metadata-file',str(u/'build-metadata.json'),'--output','type=local,dest='+str(u/'receipt'),'-f',str(u/'Dockerfile'),str(u)];(u/'command.json').write_text(json.dumps({'argv':a,'test_network':'none','GOMAXPROCS':2,'GOMEMLIMIT':'512MiB per Go process','client_cgroup':'client2CPU/2GiB; actual RUN separately2CPU/2GiB/no extra swap'},indent=2)+'\n');start=time.monotonic()
with(u/'run.log').open('w')as f:rc=subprocess.run(a,stdout=f,stderr=subprocess.STDOUT).returncode
result={'build_exit':rc,'seconds':time.monotonic()-start}
for name in ['test.rc','source-check.rc','entry-build.rc']:
 p=u/'receipt'/name;result[name]=int(p.read_text().strip())if p.exists()else None
result['pass']=rc==0 and result['test.rc']==0 and result['source-check.rc']==0 and result['entry-build.rc']==0;(u/'result.json').write_text(json.dumps(result,indent=2)+'\n');sys.exit(0 if result['pass']else 1)
