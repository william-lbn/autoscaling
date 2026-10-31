from pathlib import Path
import json,subprocess
p=Path(__file__).parent;tool='sha256:cb6635e1790c7ceae132b6a75a61e85e33be1fde8dbd9c421a17a33cd2494b9d'
cmd=['docker','run','--rm','--pull=never','--network','none','--memory','512m','--memory-swap','512m','--cpus','1','-v',str(p)+':/proof:rw','--entrypoint','genisoimage',tool,'-quiet','-J','-R','-o','/proof/runtime.iso','/proof/export/fixture/isofiles']
(p/'iso-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with(p/'iso.log').open('wb')as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
(p/'iso.exitcode').write_text(str(r.returncode)+'\n')
if r.returncode:raise SystemExit(r.returncode)
with(p/'native-driver.log').open('wb')as f:r=subprocess.run(['python3',str(p/'run-kvm.py')],stdout=f,stderr=subprocess.STDOUT)
raise SystemExit(r.returncode)
