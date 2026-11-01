from pathlib import Path
import datetime,json,os,subprocess
p=Path(__file__).parent
raw=subprocess.run(['/bin/bash','-c','set -a; source /data/neon-build/config/env.sh >/dev/null; env -0'],capture_output=True,check=True).stdout
env=os.environ.copy();env.update({k.decode():v.decode()for x in raw.split(b'\0')if x and b'='in x for k,v in [x.split(b'=',1)]})
for key in ['HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy']:
    assert env.get(key)=='http://127.0.0.1:7890',key+' must be explicit project proxy'
probe=subprocess.run(['curl','--proxy','http://127.0.0.1:7890','--connect-timeout','4','--max-time','12','--head','--silent','--show-error','--output','/dev/null','--write-out','%{http_code}','https://dl-cdn.alpinelinux.org/alpine/v3.19/main/x86_64/APKINDEX.tar.gz'],capture_output=True,text=True)
(p/'proxy-probe.json').write_text(json.dumps({'proxy':'http://127.0.0.1:7890','exit':probe.returncode,'HTTP_status':probe.stdout,'stderr':probe.stderr,'cache_may_allow_build_even_if_probe_fails':True},indent=2)+'\n')
cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=2g','--resource','memory-swap=2g','--resource','cpu-quota=200000','--network','host','--allow','network.host','--progress','plain']
for key in ['HTTP_PROXY','HTTPS_PROXY','NO_PROXY','http_proxy','https_proxy','no_proxy']:cmd+=['--build-arg',key]
cmd+=['--build-arg','TOOLS_BASE=127.0.0.1:5001/neon/ordinary-v2-tools-base@sha256:bd15f43fe3c7fc4e3c09b8cab3595ec33b25ed8a7a4d9b47ee0d23556b0a9886','--build-arg','NONCE='+p.name,'--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'export'),'-f',str(p/'Dockerfile'),str(p/'source')]
(p/'build-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
start=datetime.datetime.now(datetime.timezone.utc)
with(p/'run.log').open('wb')as log:rc=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
exits={n:int((p/'export'/n).read_text())for n in ['test.exitcode','source.exitcode','codec-build.exitcode','codec.exitcode']if(p/'export'/n).is_file()}
result={'build_exit':rc,'phase_exits':exits,'seconds':(datetime.datetime.now(datetime.timezone.utc)-start).total_seconds(),'Source':False,'normal':False,'E':0}
(p/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(0 if rc==0 and len(exits)==4 and all(v==0 for v in exits.values())else 1)
