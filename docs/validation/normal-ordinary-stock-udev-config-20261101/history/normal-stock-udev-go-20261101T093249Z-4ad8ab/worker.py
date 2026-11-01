from pathlib import Path
import subprocess,json,shlex,datetime,hashlib
p=Path(__file__).resolve().parent
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=3g','--resource','memory-swap=3g','--resource','cpu-quota=300000','--build-arg','NONCE='+p.name,'--file',str(p/'Dockerfile'),'--output','type=local,dest='+str(p/'export'),str(p/'source')]
(p/'build-command.json').write_text(json.dumps({'argv':cmd,'env_values_not_recorded':True},indent=2)+'\n')
with(p/'build.log').open('w')as log:
 rc=subprocess.run(['/bin/bash','-c','set -eu; source /data/neon-build/config/env.sh; exec '+shlex.join(cmd)],stdout=log,stderr=subprocess.STDOUT).returncode
out={'build_exit':rc,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'PURE_STATIC_COMPONENT_NO_SOURCE_NORMAL_E'}
for n in ['source','test','linux-compile','darwin-compile']:
 f=p/'export'/(n+'.exitcode');out[n+'_exit']=int(f.read_text())if f.exists()else None
(p/'result.json').write_text(json.dumps(out,indent=2)+'\n')
raise SystemExit(0 if all(out.get(n+'_exit')==0 for n in ['source','test','linux-compile','darwin-compile'])and rc==0 else 1)
