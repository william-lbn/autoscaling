from pathlib import Path
import subprocess,json,time,datetime
out=Path(__file__).parent;start=time.monotonic()
with(out/'build.log').open('wb')as log:build=subprocess.run(json.loads((out/'command.json').read_text()),stdout=log,stderr=subprocess.STDOUT)
receipt=out/'receipt';number=lambda n:int((receipt/n).read_text().strip())if(receipt/n).exists()else None
result={'build_exit':build.returncode,'test_exit':number('test.rc'),'native_build_exit':number('native-build.rc'),'seconds':time.monotonic()-start,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'normal_authorized':False,'E':0}
(out/'result.json').write_text(json.dumps(result,indent=2)+chr(10))
raise SystemExit(0 if build.returncode==0 and result['test_exit']==0 and result['native_build_exit']==0 else 1)
