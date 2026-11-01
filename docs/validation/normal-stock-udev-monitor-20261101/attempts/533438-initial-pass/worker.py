from pathlib import Path
import subprocess,json,shlex,hashlib,tarfile,io,shutil,time,traceback
p=Path(__file__).parent;r=Path('/data/neon-build/src/autoscaling');meta=json.loads((p/'metadata.json').read_text());image=meta['image'];t=time.monotonic();sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
result={'scope':meta['scope'],'normal':False,'S':0,'format_exit':None,'build_exit':None}
try:
 fdir=p/'format';fdir.mkdir()
 for n,h in meta['owned_prefmt'].items():
  assert sha(r/n)==h,n;shutil.copyfile(r/n,fdir/Path(n).name)
 fmt=['docker','run','--rm','--network','none','--cpus','3','--memory','3g','--memory-swap','3g','--read-only','--mount','type=bind,src='+str(fdir)+',dst=/format','--entrypoint','/bin/sh',image,'-ec','go version; gofmt -w /format/*.go']
 (p/'format-command.json').write_text(json.dumps(fmt,indent=2)+'\n')
 with(p/'format.log').open('w')as log:result['format_exit']=subprocess.run(fmt,stdout=log,stderr=subprocess.STDOUT).returncode
 assert result['format_exit']==0,'formatter environment failure'
 src=p/'source';src.mkdir();archive=subprocess.check_output(['git','archive','--format=tar',meta['base']],cwd=r)
 with tarfile.open(fileobj=io.BytesIO(archive))as tf:tf.extractall(src,filter='data')
 del archive
 layers={}
 for label,origin,names in [
  ('ordinary11',Path('/data/neon-build/validation/normal-metadata-wiring-ordinary-udev-uuid36-20261101T085636Z-aaa1cc/source'),None),
  ('stock4',Path('/data/neon-build/validation/normal-stock-udev-go-20261101T093936Z-963edc/source'),['normal_ordinary_stock_udev_manifest_linux.go','normal_ordinary_stock_udev_manifest_linux_test.go','normal_ordinary_stock_udev_projection_linux.go','normal_ordinary_stock_udev_projection_linux_test.go'])]:
  if names is None:names=[q.name for q in (origin/'pkg/dsga/resourceguard').glob('normal_ordinary_udev_v2_*.go')]
  layers[label]={}
  for name in names:
   n='pkg/dsga/resourceguard/'+name;shutil.copyfile(origin/n,src/n);layers[label][n]=sha(src/n)
 for n,h in meta['owned_prefmt'].items():
  assert sha(r/n)==h,n;shutil.copyfile(fdir/Path(n).name,r/n);shutil.copyfile(r/n,src/n)
 files={str(f.relative_to(src)):sha(f)for f in sorted(src.rglob('*'))if f.is_file()and not f.is_symlink()and f.suffix in ['.go','.s','.S','.mod','.sum']}
 (src/'actual-source-sha256.txt').write_text(''.join(h+'  '+n+'\n'for n,h in files.items()));(src/'.dockerignore').write_text('.git\n')
 lock={'base':meta['base'],'owned3':{n:sha(src/n)for n in meta['owned_prefmt']},'immutable_layers':layers,'actual_go_inputs':files,'other_WIP_excluded':True,'scope':meta['scope']};(p/'source-lock.json').write_text(json.dumps(lock,indent=2)+'\n')
 docker='FROM '+image+' AS test\nWORKDIR /workspace\nENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=768MiB\nCOPY . .\nARG NONCE\nRUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; test "$(cat /sys/fs/cgroup/memory.max)" = 3221225472; test "$(cat /sys/fs/cgroup/cpu.max)" = "300000 100000"; mkdir /out; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; go version >/out/go-version.txt; sha256sum -c actual-source-sha256.txt >/out/actual-copy-verification.txt; echo 0 >/out/source.exitcode; cp actual-source-sha256.txt /out/compiled-source-sha256.txt; set +e; go test -race -count=1 -timeout=180s -json -run "^TestNormalStockUdevMonitor" ./pkg/dsga/resourceguard >/out/test.json 2>/out/test.stderr; echo $? >/out/test.exitcode; CGO_ENABLED=0 go build -o /out/resource-guard ./neonvm-daemon/cmd/resource-guard >/out/cli.stdout 2>/out/cli.stderr; echo $? >/out/cli.exitcode; GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c -o /out/darwin.test ./pkg/dsga/resourceguard >/out/darwin.stdout 2>/out/darwin.stderr; echo $? >/out/darwin.exitcode; CGO_ENABLED=0 go test -c -tags dsga_native_stock_udev_sticky -o /out/sticky-native.test ./pkg/dsga/resourceguard >/out/native.stdout 2>/out/native.stderr; echo $? >/out/native.exitcode\nFROM scratch\nCOPY --from=test /out/ /\n'
 (p/'Dockerfile').write_text(docker)
 cmd=['docker','buildx','build','--builder','neon-data','--resource','memory=3g','--resource','memory-swap=3g','--resource','cpu-quota=300000','--build-arg','NONCE='+p.name,'--file',str(p/'Dockerfile'),'--output','type=local,dest='+str(p/'export'),str(src)]
 (p/'build-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
 with(p/'build.log').open('w')as log:result['build_exit']=subprocess.run(['/bin/bash','-c','set -eu; source /data/neon-build/config/env.sh; exec '+shlex.join(cmd)],stdout=log,stderr=subprocess.STDOUT).returncode
 for n in ['source','test','cli','darwin','native']:
  f=p/'export'/(n+'.exitcode');result[n+'_exit']=int(f.read_text())if f.exists()else None
 result['source_lock_sha256']=sha(p/'source-lock.json')
except Exception:
 (p/'driver-error.txt').write_text(traceback.format_exc());result['driver_error']=True
result['seconds']=time.monotonic()-t;(p/'result.json').write_text(json.dumps(result,indent=2)+'\n')
raise SystemExit(0 if result.get('build_exit')==0 and all(result.get(n+'_exit')==0 for n in ['source','test','cli','darwin','native']) else 1)
