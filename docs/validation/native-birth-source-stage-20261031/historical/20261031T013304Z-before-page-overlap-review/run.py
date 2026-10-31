from pathlib import Path
import subprocess,json,time
p=Path(__file__).parent
cmd=['docker','buildx','build','--builder','neon-data','--network','none','--progress','plain','--build-arg','20261031T011639Z=20261031T011639Z','--metadata-file',str(p/'build-metadata.json'),'--output','type=local,dest='+str(p/'artifact'),'-f',str(p/'Dockerfile'),'/data/neon-build/dev/native-birth-source-elf-fix-20261031T013304Z']
(p/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
tick=time.time()
with(p/'run.log').open('w')as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,timeout=300)
(p/'run-result.json').write_text(json.dumps({'exit':r.returncode,'seconds':time.time()-tick,'scope':'U real Linux private WAL/fsync with explicit model source/kernel; NOT S/E'},indent=2)+'\n')
raise SystemExit(r.returncode)
