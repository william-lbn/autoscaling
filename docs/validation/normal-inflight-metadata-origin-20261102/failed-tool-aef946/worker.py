from pathlib import Path
import subprocess,json,os,time,re,hashlib,signal
p=Path(__file__).parent;v=p.parent;start=time.monotonic();env=dict(os.environ);raw=subprocess.run(['/bin/bash','-c','set -a\nsource /data/neon-build/config/env.sh >/dev/null\nenv -0'],capture_output=True,check=True).stdout
allowed={'PATH','DOCKER_CONFIG','DOCKER_HOST','DOCKER_CONTEXT','BUILDX_BUILDER','HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy','NO_PROXY','no_proxy'}
env.update({k.decode():z.decode()for x in raw.split(b'\0')if x and b'='in x for k,z in[x.split(b'=',1)]if k.decode()in allowed})
for k in ['HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy']:assert env.get(k)=='http://127.0.0.1:7890'
old=v/'normal-inflight-compiled-abi-20261031T172033Z-d84115/export/kernel-debug.elf'
# Small actual compiled369 DWARF parser control before using this reader on18c.
probe=['docker','run','--rm','--pull=never','--network=none','--read-only','--cpus=1','--memory=512m','--memory-swap=512m','--cap-drop=ALL','--security-opt=no-new-privileges','-v',str(p)+':/probe:rw','-v',str(old)+':/kernel-debug.elf:ro','--entrypoint=python3','sha256:ebaeba1cc3372b303db000f2379d75e466dd0bcc86705caa716c7c9d42962d7b','/probe/dwarf_read.py','/kernel-debug.elf','/probe/compiled369-parser-control.json'];(p/'probe-command.json').write_text(json.dumps(probe,indent=2)+'\n')
with(p/'probe.log').open('w')as log:rc=subprocess.run(probe,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=90).returncode
if rc: (p/'result.json').write_text(json.dumps({'probe_exit':rc,'build_not_started':True,'reason':'tool unsupported stop per plan'})+'\n');raise SystemExit(rc)
cmd=json.loads((v/'normal-kernel-metadata-build-20261101T023713Z-58603b/build/command.json').read_text());new=[];i=0
while i<len(cmd):
 x=cmd[i]
 if x=='--resource':
  val=cmd[i+1];val={'memory=6g':'memory=1g','memory-swap=6g':'memory-swap=1g','cpu-quota=400000':'cpu-quota=200000'}.get(val,val);new +=[x,val];i+=2;continue
 if x in ['--target','--metadata-file','--output']:
  val={'--target':'inflight_metadata_export','--metadata-file':str(p/'build-metadata.json'),'--output':'type=local,dest='+str(p/'export')}[x];new +=[x,val];i+=2;continue
 if i==len(cmd)-1:new+=['--build-context','readback_input='+str(p),'-f',str(p/'Dockerfile'),x];i+=1;continue
 new.append(x);i+=1
(p/'command.json').write_text(json.dumps(new,indent=2)+'\n');q=subprocess.Popen(new,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,start_new_session=True);aborted=False;cached=set();oldruns=set();lines=[]
with(p/'build.log').open('w')as log:
 for l in q.stdout:
  log.write(l);log.flush();lines.append(l)
  m=re.match(r'#(\d+) \[(?:build_amd64|normal_split_build|control_metadata_build|build-deps|check-arg)[^\]]*\] RUN ',l)
  if m:oldruns.add(m.group(1))
  m=re.match(r'#(\d+) CACHED',l)
  if m:cached.add(m.group(1))
  # Any output from an old RUN means it executed, not CACHED: stop immediately.
  m=re.match(r'#(\d+) [0-9]+\.[0-9]+ ',l)
  if m and m.group(1)in oldruns:
   aborted=True;os.killpg(q.pid,signal.SIGTERM);break
rc=q.wait(timeout=30);result={'probe_exit':0,'build_exit':rc,'cache_only_aborted':aborted,'old_RUN_steps':sorted(oldruns),'old_RUN_cached':sorted(oldruns&cached),'all_seen_old_RUN_cached':oldruns<=cached,'seconds':time.monotonic()-start,'normal_qualified':False,'E':0};(p/'result.json').write_text(json.dumps(result,indent=2)+'\n')
if rc or aborted or not oldruns<=cached:raise SystemExit(rc or 1)
exp=p/'export';a=(v/'normal-inflight-kernel-source-20261031T172846Z-c8a045/export/actual-build-source-SHA256SUMS').read_text().splitlines();b=(exp/'source36-SHA256SUMS').read_text().splitlines();oldmap={l.split('  ',1)[1]:l.split('  ',1)[0]for l in a};newmap={l.split('  ',1)[1]:l.split('  ',1)[0]for l in b};delta={n:[oldmap.get(n),h]for n,h in newmap.items()if oldmap.get(n)!=h};assert len(newmap)==36
abi=json.loads((exp/'dwarf-types.json').read_text());sizes={n:sorted(set(x['byte_size']for x in xs))for n,xs in abi['types'].items()};assert not delta,delta
sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest();result.update({'source36_exact_old369_match':not delta,'source36_delta':delta,'dwarf_actual_sizes':sizes,'export_sha256':{f.name:sha(f)for f in exp.iterdir()if f.is_file()}});assert result['export_sha256']['kernel.config']=='966e615127358d6d0dcf4f6ca884a07702e957ad07a95909cf95feca4aae5441';assert result['export_sha256']['kernel-notes.bin']=='a496a3c44fc0b374f100ba0331a2e21f02d6e79ac999769a8d3ad4d71b76024e';(p/'result.json').write_text(json.dumps(result,indent=2)+'\n')
