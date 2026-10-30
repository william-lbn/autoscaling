from pathlib import Path
import json,hashlib,re
out=Path("/data/neon-build/validation/pg17-pid-capped-kernel-20261030T175217Z");p=out/'actual-source-export-attempt02'
lines=(p/'actual-source-SHA256SUMS').read_text().splitlines()
sums={l.split(None,1)[1]:l.split(None,1)[0] for l in lines}
build=json.loads((out/'build-result.json').read_text())
assert sums['/build/linux-6.12.26/arch/x86/boot/bzImage']==build['vmlinuzSha256']
assert sums['/build/linux-6.12.26/.config']==build['resolvedConfigSha256']
manifest=json.loads((out/'kernel-context-source-manifest.json').read_text())
for f in (p/'patches').iterdir():assert hashlib.sha256(f.read_bytes()).hexdigest()==manifest['patches/'+f.name]['sha256']
for name,path in [('pid.h','include/linux/pid.h'),('pid.c','kernel/pid.c'),('sysctl.c','kernel/sysctl.c')]:
 assert hashlib.sha256((p/'patched-kernel'/name).read_bytes()).hexdigest()==sums['/build/linux-6.12.26/'+path]
patches={f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in (p/'patches').iterdir()}
digest=hashlib.sha256(json.dumps(patches,sort_keys=True,separators=(',',':')).encode()).hexdigest()
log=(out/'actual-source-build-attempt02.log').read_text()
proof={'status':'PASS_SOURCE_RECEIPT','scope':'fresh actual-source hash RUN from exact compiled cache layer; not another runtime test','actualSourceSHA256':sums,'patchSetSHA256':digest,'patchSetDigestAlgorithm':'sha256 of sorted compact JSON mapping patch filenames to sha256','actualPatchFilesMatchImmutableContext':True,'sameCompiledArtifactAndResolvedConfig':True,'kernelMakeStepCached':'#14 CACHED' in log and '#14 [build_amd64 2/2]' in log,'aptStepCached':'#11 CACHED' in log and '#11 [build-deps 3/8]' in log,'freshReceiptRUNNetworkNone':'#15 DONE 0.2s' in log and '#15 [actual_source_receipt 1/1] RUN --network=none' in log,'sourceBuildMetadataSHA256':hashlib.sha256((out/'actual-source-build-metadata-attempt02.json').read_bytes()).hexdigest(),'sourceBuildLogSHA256':hashlib.sha256((out/'actual-source-build-attempt02.log').read_bytes()).hexdigest(),'preparationFailuresRetained':[{'attempt':1,'proof':'actual-source-proof.json','reason':'Global network=none changed all upstream cache keys; APT offline failure occurred before make. Original old caches retained.'},{'attempt':2,'reason':'Build/export succeeded, then Python postprocessor referenced old export path. No rebuild/rerun; this evaluator reads existing correct attempt02 export.'}]}
assert proof['kernelMakeStepCached'] and proof['aptStepCached'] and proof['freshReceiptRUNNetworkNone']
(out/'actual-source-proof-final.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof,indent=2))
