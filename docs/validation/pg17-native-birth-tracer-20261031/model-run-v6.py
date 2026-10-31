from pathlib import Path
import subprocess,json,time
p=Path(__file__).parent
cmd=['docker','buildx','build','--builder','neon-data','--network','none','--progress','plain','--build-arg','NONCE=model-pidfd-v6-20261031','--metadata-file',str(p/'model-v6-build-metadata.json'),'--output','type=local,dest='+str(p/'model-v6-artifact'),'-f',str(p/'Dockerfile-model-v6'),'/data/neon-build/dev/pg17-go-native-tracer-20261030T235629Z/cohort-410bcc']
(p/'model-v6-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
started=time.time()
with (p/'model-v6-run.log').open('w') as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,timeout=300)
(p/'model-v6-run-result.json').write_text(json.dumps({'exit':r.returncode,'seconds':time.time()-started,'scope':'MODEL_UNIT_NOT_REAL_KERNEL'},indent=2)+'\n')
raise SystemExit(r.returncode)
