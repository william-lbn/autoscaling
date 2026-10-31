from pathlib import Path
import subprocess,json
out=Path('/data/neon-build/validation/normal-split-pid17-20261030T225434Z')
with (out/'run.log').open('w') as log:
 r=subprocess.run(['/usr/bin/python3',str(out/'run-kvm-matrix.py')],stdout=log,stderr=subprocess.STDOUT)
 e=subprocess.run(['/usr/bin/python3',str(out/'evaluate-runtime-proof.py')],stdout=log,stderr=subprocess.STDOUT)
code=0 if r.returncode==0 and e.returncode==0 else 1
(out/'exitcode').write_text(str(code)+'\n')
print(json.dumps({'matrix_exit':r.returncode,'evaluator_exit':e.returncode,'exitcode':code}),flush=True)
raise SystemExit(code)
