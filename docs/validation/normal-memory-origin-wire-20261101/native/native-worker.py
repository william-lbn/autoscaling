from pathlib import Path
import subprocess,json,time
d=Path(__file__).parent;t=time.monotonic();cmd=json.loads((d/'native-command.json').read_text())
with (d/'native-host.log').open('wb') as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,timeout=180)
inspect=subprocess.run(['docker','inspect',cmd[3]],capture_output=True,text=True)
(d/'container-inspect.json').write_text(inspect.stdout)
v={'docker_rc':r.returncode,'seconds':time.monotonic()-t,'normal_authorized':False,'E':0};(d/'native-result.json').write_text(json.dumps(v,indent=2)+'\n')
(d/'guest-wal').mkdir()
with (d/'guest-wal-extract.log').open('wb') as f:subprocess.run(['debugfs','-R','rdump / '+str(d/'guest-wal'),str(d/'state.ext4')],stdout=f,stderr=subprocess.STDOUT)
raise SystemExit(r.returncode)
