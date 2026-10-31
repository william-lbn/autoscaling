from pathlib import Path
import subprocess,json,time,hashlib
p=Path(__file__).parent;c=json.loads((p/'command.json').read_text());start=time.monotonic()
with (p/'run.log').open('wb') as f:r=subprocess.run(c,stdout=f,stderr=subprocess.STDOUT)
a={'component':'actual statically compiled builder final inventory command on explicitly synthetic root files','exit':r.returncode,'seconds':time.monotonic()-start,'actual_image_build':False,'actual_services':False,'normal_permit':False}
if (p/'generated-manifest.json').exists():
 raw=(p/'generated-manifest.json').read_bytes();m=json.loads(raw);a.update({'generated_manifest_sha256':hashlib.sha256(raw).hexdigest(),'protocol_version':m['protocol_version'],'profile_id':m['profile_id'],'files':len(m['files']),'declared_image':'explicit synthetic ab model, not actual image digest','repeat_exit':int((p/'repeat.exitcode').read_text())})
(p/'result.json').write_text(json.dumps(a,indent=2)+'\n');raise SystemExit(r.returncode)
