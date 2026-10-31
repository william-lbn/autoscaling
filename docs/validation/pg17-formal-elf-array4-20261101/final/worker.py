from pathlib import Path
import json,subprocess,time
p=Path(__file__).parent;t=time.monotonic()
with(p/'stdout.log').open('wb')as o,(p/'stderr.log').open('wb')as e:r=subprocess.run(json.loads((p/'command.json').read_text()),stdout=o,stderr=e)
(p/'result.json').write_text(json.dumps({'exit':r.returncode,'seconds':time.monotonic()-t,'scope':'actual formal9f3 PG/map_files with OS-account adapter; not Root/normal/KVM','E':0},indent=2)+'\n')
raise SystemExit(r.returncode)
