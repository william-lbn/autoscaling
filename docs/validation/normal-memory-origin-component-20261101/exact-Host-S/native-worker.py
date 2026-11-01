from pathlib import Path
import json,subprocess,time,hashlib,datetime
d=Path(__file__).parent;t=time.monotonic();cmd=json.loads((d/'native-command.json').read_text())
with (d/'native.log').open('wb') as f:z=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,timeout=205)
result={'native_exit':z.returncode,'pass':z.returncode==0,'seconds':time.monotonic()-t,'normal_qualified':False,'E':0,'scope':'Host new opaque memory origin actual KVM/QMP component; Guest new6/7 NOT_RUN'}
(d/'native-result.json').write_text(json.dumps(result,indent=2)+'\n')
r=subprocess.run(['docker','inspect','normal-memory-origin-host-ec5ace'],capture_output=True,text=True)
if r.returncode==0:(d/'native-container-inspect.json').write_text(r.stdout)
else:(d/'native-container-inspect-error.txt').write_text(r.stderr)
raise SystemExit(0 if z.returncode==0 else 1)
