from pathlib import Path
import subprocess,json,time,hashlib,shutil
p=Path(__file__).parent
original=Path('/data/neon-build/validation/normal-role-authority-v2-final-native-build-20261101T054417Z-dfe59c')
for i in range(240):
 f=original/'export/component.exitcode'
 if f.exists():
  if f.read_text().strip()!='0':raise SystemExit('original immutable build failed; legacy NotRun')
  break
 time.sleep(1)
else:raise SystemExit('immutable build unavailable; legacy NotRun')
(p/'export').mkdir();shutil.copy2(original/'export/service.test',p/'export/service.test');shutil.copy2(original/'export/resource-guard.real',p/'export/resource-guard.real')
manifest=json.loads((original/'source-lock.json').read_text());(p/'source-lock.json').write_text(json.dumps({'original_cohort':str(original),'original_source_lock_sha256':hashlib.sha256((original/'source-lock.json').read_bytes()).hexdigest(),'compiled_binary_sha256':hashlib.sha256((p/'export/service.test').read_bytes()).hexdigest(),'owned':manifest['owned'],'runtime_declaration':1,'scope':'same exact Go ELF, old declaration1 actual R3 ServeGuest; no Source/normal/E'},indent=2)+'\n')
T='127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1'
cmd=['docker','run','--rm','--pull=never','--network=none','--read-only','--memory=1g','--memory-swap=1g','--cpus=2','-v',str(original/'source')+':/workspace:ro','-v',str(original/'export/service.test')+':/compiled/service.test:ro','-v',str(p)+':/proof:rw','--workdir','/workspace','-e','DSGA_SYSTEM_ROLE_ELF=/compiled/service.test','-e','DSGA_SERVICE_GUARD_ELF=/compiled/service.test','-e','DSGA_SYSTEM_LIFECYCLE_EXPORT=/proof/export/fixture','-e','DSGA_TEST_BROKER_DECLARATION_VERSION=1','--entrypoint','/compiled/service.test',T,'-test.run','^TestExportNormalSystemRoleServiceFixture$','-test.v'];(p/'fixture-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with(p/'fixture.log').open('wb')as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
(p/'fixture.exitcode').write_text(str(r.returncode)+'\n')
if r.returncode:raise SystemExit(r.returncode)
with(p/'native-driver.log').open('wb')as f:r=subprocess.run(['python3',str(p/'run-native.py'),str(p)],stdout=f,stderr=subprocess.STDOUT)
raise SystemExit(r.returncode)
