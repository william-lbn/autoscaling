from pathlib import Path
import json,subprocess,hashlib,sys
dev=Path(__file__).parent;cfg=json.loads((dev/'config.json').read_text());out=Path(cfg['out'])
r=subprocess.run(['/usr/bin/python3',str(dev/'run.py'),str(dev/'config.json')])
if r.returncode:sys.exit(r.returncode)
prior=out.parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(out/'export/guest.test')==sha(prior/'export/guest.test'),'new Guest binary differs: must rebuild initramfs'
lock=json.loads((out/'component-input-lock.json').read_text());lock['HostStaticSHA256']=sha(out/'export/host.test');lock['GuestStaticSHA256']=sha(out/'export/guest.test');lock['immutableGuestISOReusedFrom']=str(prior);lock['GuestBinaryExactSame']=True
(out/'component-input-lock.json').write_text(json.dumps(lock,indent=2)+'\n')
r=subprocess.run(['/usr/bin/python3',str(dev/'run-kvm.py')]);sys.exit(r.returncode)
