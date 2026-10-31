from pathlib import Path
import json,subprocess,time,datetime
u=Path(__file__).resolve().parent;start=time.monotonic();cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=2g','--resource','memory-swap=2g','--resource','cpu-quota=200000','--progress','plain','--output','type=local,dest='+str(u/'receipt'),'-f',str(u/'Dockerfile'),str(u/'source')];(u/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');result={'normal_qualified':False,'E':0}
try:
 with (u/'build.log').open('wb') as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,timeout=1200)
 result['build_exit']=r.returncode
 for k in ['test','agent']:result[k+'_exit']=int((u/'receipt'/(k+'.rc')).read_text()) if (u/'receipt'/(k+'.rc')).exists() else None
 result['pass']=result['build_exit']==result['test_exit']==result['agent_exit']==0
except BaseException as e:result['error']=repr(e);result['pass']=False
result['seconds']=time.monotonic()-start;result['endUTC']=datetime.datetime.now(datetime.timezone.utc).isoformat();(u/'result.json').write_text(json.dumps(result,indent=2)+'\n');raise SystemExit(0 if result['pass'] else 1)
