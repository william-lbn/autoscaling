from pathlib import Path
import subprocess,json,time,hashlib
d=Path(__file__).parent;t=time.monotonic()
with(d/'build.log').open('wb')as log:p=subprocess.run(json.loads((d/'command.json').read_text()),stdout=log,stderr=subprocess.STDOUT)
files={str(f.relative_to(d/'export')):hashlib.sha256(f.read_bytes()).hexdigest()for f in(d/'export').rglob('*')if f.is_file()}if(d/'export').exists()else{}
(d/'result.json').write_text(json.dumps({'source_export_exit':p.returncode,'seconds':time.monotonic()-t,'kernel_recompiled':False,'new_patch_applied':False,'files':files},indent=2)+chr(10))
raise SystemExit(p.returncode)
