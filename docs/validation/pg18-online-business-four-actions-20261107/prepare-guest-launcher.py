from pathlib import Path as P
import os,json,subprocess as s,hashlib,time
os.umask(0o077);d=P(__file__).parent;src=P('/data/neon-build/validation/PG18-正式独立Guest-940b25fb');os.environ['DOCKER_CONFIG']='/data/neon-build/config/docker'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
s.run(['cp','-a','--reflink=auto',str(src/'context'),str(d/'context')],check=True)
for n in ['production-build-plan.json','base.Dockerfile.template','guest.Dockerfile','guest.Dockerfile.dockerignore','original-pack-worker.py']:
 (d/n).write_bytes((src/n).read_bytes())
c=json.loads((src/'inputs.json').read_text());c.update(base='127.0.0.1:5001/neon-candidate/pg18-base:launcher-940b25fb',guest='127.0.0.1:5001/neon-candidate/pg18-guest:launcher-940b25fb',nonce='pg18-launcher-940b25fb')
(d/'inputs.json').write_text(json.dumps(c,indent=2));builder=P(c['builder']);assert sha(builder)=='8e4999184fe060d2813a4e137609ad18f6ce903e973fad05167bc74e8b5e3e4e'
root=d/'context/root';original=(root/'neonvm/bin/vmstart').read_bytes();assert b'--compute-id cp-d8d85ed796068ab5' in original
(d/'original-launcher.private.txt').write_bytes(original)
cmd=['docker','run','--rm','--pull=never','--network=none','--cpus=2','--memory=3g','--memory-swap=3g','-v',str(d)+':/proof:rw','-v',str(builder)+':/builder:ro','--entrypoint','/builder',c['tools'],'--target-arch=linux/amd64','--system-mode=normal-split-v3','--normal-manifest-root=/proof/context/root','--normal-build-plan=/proof/production-build-plan.json','--normal-prepare-trusted-compute-only','--normal-ordinary-controls-version=2']
with(d/'prepare-original-launcher.log').open('xb')as f:q=s.run(cmd,stdout=f,stderr=f,timeout=90)
assert q.returncode==0
actual=(root/'neonvm/bin/vmstart').read_bytes()
assert b'--compute-id cp-24c41e23bb104ead' in actual and b'cp-d8d85ed796068ab5' not in actual
assert actual==original.replace(b'cp-d8d85ed796068ab5',b'cp-24c41e23bb104ead')
(d/'launcher-readback.PUBLIC.json').write_text(json.dumps({'old_SHA256':hashlib.sha256(original).hexdigest(),'new_SHA256':hashlib.sha256(actual).hexdigest(),'compute_id':'cp-24c41e23bb104ead','actual_original_Builder_flag':'normal-prepare-trusted-compute-only','PG_ELF_unchanged':sha(root/'usr/local/bin/postgres')=='162e8f20c7a2e2b34f25b74288ee495e61fd70519d62829ab1899ca8501bd4ef'},indent=2))
s.run(['python3','-u',str(d/'original-pack-worker.py')],check=True,timeout=1000)
