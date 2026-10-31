from pathlib import Path
import subprocess,json,os,time,hashlib
p=Path(__file__).parent
raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout
env=os.environ.copy();env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in [x.split(b'=',1)]})
tag='127.0.0.1:5001/neon/compute-v17-array-test:20261031t202515z-78e2ac'
cmd=['docker','buildx','build','--builder','neon-data','--network','host','--allow','network.host','--progress','plain','--resource','memory=2g','--resource','memory-swap=2g','--resource','cpu-quota=200000','--build-arg','NONCE='+p.name,'--build-arg','HTTP_PROXY','--build-arg','HTTPS_PROXY','--build-arg','NO_PROXY','--load','-t',tag,str(p/'context')]
(p/'build-command.json').write_text(json.dumps(cmd,indent=2)+'\n');t=time.monotonic()
with(p/'build.log').open('wb')as f:r=subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT)
(p/'build-result.json').write_text(json.dumps({'exit':r.returncode,'seconds':time.monotonic()-t},indent=2)+'\n')
if r.returncode:raise SystemExit(r.returncode)
x=json.loads(subprocess.check_output(['docker','image','inspect',tag]))[0];(p/'image-identity.json').write_text(json.dumps({'Id':x['Id'],'RepoDigests':x['RepoDigests'],'tag':tag},indent=2)+'\n')
subprocess.run(['docker','run','--rm','--network','none','--memory','512m','--memory-swap','512m','--cpus','1','--entrypoint','/bin/sh','-v',str(p)+':/proof:rw',x['Id'],'-c','cp -a /test-build-proof /proof/compiled-proof'],check=True)
cmd=['docker','run','--init','--rm','--pull=never','--network','none','--tmpfs','/tmp:rw,size=512m,mode=1777','--cpus','4','--memory','5g','--memory-swap','5g','--pids-limit','512','--user','0:0','--cap-add','SYS_ADMIN','--cap-add','SYS_PTRACE','--security-opt','seccomp=unconfined','--entrypoint','/usr/bin/env','-v',str(p)+':/test:rw','-e','NEON_TEST_PG_BIN=/usr/local/bin','-e','NEON_TEST_DSG_LIBRARY=/usr/local/pgsql/lib/test_dsg_region.so','-e','NEON_ARRAY_ROOT_EVIDENCE=/test/evidence','-w','/test',x['Id'],'python3','test_array_root.py']
(p/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');t=time.monotonic()
with(p/'stdout.log').open('wb')as o,(p/'stderr.log').open('wb')as e:r=subprocess.run(cmd,stdout=o,stderr=e)
(p/'result.json').write_text(json.dumps({'exit':r.returncode,'seconds':time.monotonic()-t,'imageId':x['Id'],'scope':'real PG ELF/map_files fixture; not Root/normal/KVM','E':0},indent=2)+'\n')
raise SystemExit(r.returncode)
