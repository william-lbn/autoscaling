from pathlib import Path
import subprocess,json,time
p=Path(__file__).parent;cmd=json.loads((p/'command.json').read_text());started=time.time()
try:
 with (p/'run.log').open('w') as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,timeout=150)
 result={'exitCode':r.returncode,'elapsedSeconds':time.time()-started,'kind':'REAL_NATIVE_HOST_COMPONENT_NOT_E2E'}
except subprocess.TimeoutExpired:
 name=cmd[cmd.index('--name')+1]
 cleanup=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True)
 result={'exitCode':124,'elapsedSeconds':time.time()-started,'kind':'REAL_NATIVE_HOST_COMPONENT_TIMEOUT','ownedContainerCleanupExit':cleanup.returncode}
(p/'run-result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(result['exitCode'])
