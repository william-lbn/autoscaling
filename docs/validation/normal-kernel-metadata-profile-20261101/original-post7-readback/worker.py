from pathlib import Path
import json,subprocess,hashlib,time
d=Path(__file__).parent;t=time.monotonic()
with(d/'build.log').open('wb')as f:p=subprocess.run(json.loads((d/'command.json').read_text()),stdout=f,stderr=subprocess.STDOUT)
files={str(p.relative_to(d/'export')):hashlib.sha256(p.read_bytes()).hexdigest()for p in(d/'export').rglob('*')if p.is_file()}if(d/'export').exists()else{}
(d/'result.json').write_text(json.dumps({'source_export_exit':p.returncode,'seconds':time.monotonic()-t,'files':files},indent=2)+chr(10))
raise SystemExit(p.returncode)
