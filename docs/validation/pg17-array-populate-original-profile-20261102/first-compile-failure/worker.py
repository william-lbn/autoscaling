from pathlib import Path
import os,subprocess,json,time
p=Path(__file__).parent
raw=subprocess.check_output(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'])
env=os.environ.copy();env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in[x.split(b'=',1)]})
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=3g','--resource','memory-swap=3g','--resource','cpu-quota=300000','--network','none','--progress','plain','--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
(p/'build-command.json').write_text(json.dumps(cmd,indent=2)+'\n');start=time.monotonic()
with(p/'build.log').open('wb')as f:rc=subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT).returncode
phases={n:int((p/'export'/n).read_text())if(p/'export'/n).exists()else None for n in ['u.exitcode','cli.exitcode','guest.exitcode']}
result={'build_exit':rc,'phases':phases,'seconds':time.monotonic()-start,'Source':False,'normal':False,'E':0}
(p/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True);raise SystemExit(0 if rc==0 and all(v==0 for v in phases.values())else 1)
