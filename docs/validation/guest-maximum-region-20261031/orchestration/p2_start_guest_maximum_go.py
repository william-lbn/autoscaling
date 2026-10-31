"""冻结Git对象+独占4源；实际race测试/静态Guest Reader导出，Root并行改动不入本快照。"""
import subprocess
REMOTE=r"""
from pathlib import Path
import datetime,subprocess,hashlib,json,tarfile,io,os
b=Path('/data/neon-build');r=b/'src/autoscaling';nonce=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=b/'validation'/('guest-maximum-go-'+nonce);out.mkdir();snap=b/'snapshots'/('guest-maximum-go-'+nonce);snap.mkdir()
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip();raw=subprocess.check_output(['git','archive','--format=tar',head],cwd=r);(out/'base-source.tar').write_bytes(raw)
with tarfile.open(fileobj=io.BytesIO(raw)) as t:t.extractall(snap,filter='data')
files=['guest_memory_maximum.go','guest_memory_maximum_linux.go','guest_memory_maximum_test.go','guest_memory_maximum_native_test.go'];owned={}
for n in files:
 rel='pkg/dsga/resourceguard/'+n;data=(r/rel).read_bytes();(snap/rel).write_bytes(data);owned[rel]=hashlib.sha256(data).hexdigest()
allgo={str(p.relative_to(snap)):hashlib.sha256(p.read_bytes()).hexdigest() for p in snap.rglob('*') if p.is_file() and (p.suffix=='.go' or p.name in ['go.mod','go.sum'])};(out/'snapshot-source.json').write_text(json.dumps({'base_head':head,'base_archive_sha256':hashlib.sha256(raw).hexdigest(),'owned_files':owned,'all_go_files':allgo,'excluded_other_uncommitted_changes':True,'scope':'immutable Git + owned4 only; component evidence'},indent=2)+'\n')
for f in snap.rglob('*'):
 if f.is_file() and not f.is_symlink():f.chmod(0o444)
docker='''FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS build
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2
COPY . .
ARG GUEST_MAXIMUM_RUN_NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared \
 printf 'ACTUAL_GUEST_MAXIMUM_RUN=%s\\n' "$GUEST_MAXIMUM_RUN_NONCE" && mkdir /out && \
 find . -type f \\( -name '*.go' -o -name go.mod -o -name go.sum \\) -print0 | sort -z | xargs -0 sha256sum > /out/copied-source-SHA256SUMS && \
 go version > /out/go-version.txt && \
 go test -mod=readonly -race -count=1 -json -run '^TestGuestMaximum' ./pkg/dsga/resourceguard | tee /out/unit.json && \
 CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -mod=readonly -trimpath -c -tags dsga_native_guest_memory_maximum -o /out/guest-maximum-native.test ./pkg/dsga/resourceguard && \
 cd /out && sha256sum guest-maximum-native.test > binary-SHA256SUMS
FROM scratch
COPY --from=build /out/ /
''';(out/'guest-maximum-test.Dockerfile').write_text(docker)
runner=r'''from pathlib import Path
import subprocess,json,os,time,hashlib
out=Path(OUT)
env=os.environ.copy();env.update(dict(x.split('=',1) for x in subprocess.check_output(['bash','-c','source /data/neon-build/config/env.sh; env -0']).decode().split('\0') if '=' in x))
cmd=['docker','buildx','build','--builder','neon-data','--network','none','--progress','plain','--build-arg','GUEST_MAXIMUM_RUN_NONCE='+out.name,'--metadata-file',str(out/'build-metadata.json'),'--output','type=local,dest='+str(out/'export'),'-f',str(out/'guest-maximum-test.Dockerfile'),SNAP]
(out/'build-command.json').write_text(json.dumps(cmd,indent=2)+'\n');start=time.monotonic()
with (out/'build.log').open('wb') as f:p=subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT)
(out/'exitcode').write_text(str(p.returncode)+'\n');(out/'build-result.json').write_text(json.dumps({'exit_code':p.returncode,'elapsed_seconds':time.monotonic()-start,'Guest_started':False,'scope':'U race and native static build only'},indent=2)+'\n');raise SystemExit(p.returncode)
'''.replace('Path(OUT)','Path('+repr(str(out))+')').replace('SNAP',repr(str(snap)))
(out/'run-build.py').write_text(runner)
unit='guest-maximum-go-'+nonce.lower();subprocess.run(['systemd-run','--unit',unit,'--property=Type=oneshot','--property=MemoryMax=4G','--property=CPUQuota=300%','--property=StandardOutput=file:'+str(out/'runner.log'),'--property=StandardError=file:'+str(out/'runner.log'),'--no-block','python3',str(out/'run-build.py')],check=True)
(b/'validation/guest-maximum-go-latest.txt').write_text(str(out)+'\n');print(json.dumps({'path':str(out),'snapshot':str(snap),'unit':unit,'owned':owned},indent=2))
"""
subprocess.run(['python3','remote.py','python3 -'],input=REMOTE,text=True,check=True)
