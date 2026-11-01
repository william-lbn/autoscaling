from pathlib import Path
import json,subprocess,time,datetime
p=Path(__file__).parent;t=time.monotonic()
with(p/'build.log').open('wb')as log:x=subprocess.run(json.loads((p/'command.json').read_text()),stdout=log,stderr=subprocess.STDOUT)
def number(n):
 f=p/'export'/n
 if not f.exists():return None
 s=f.read_text().strip()
 return int(s)if s.lstrip('-').isdigit()else s
z={'build_exit':x.returncode,'test_exit':number('test.rc'),'native_build_exit':number('native-build.rc'),'fixture_exit':number('fixture.rc'),'probe_build_exit':number('probe-build.rc'),'seconds':time.monotonic()-t,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'normal_authorized':False,'E':0};(p/'result.json').write_text(json.dumps(z,indent=2)+'\n');raise SystemExit(0 if all(z[k]==0 for k in ['build_exit','test_exit','native_build_exit','fixture_exit','probe_build_exit'])else 1)
