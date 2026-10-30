from pathlib import Path
import json,re,hashlib,base64
out=Path("/data/neon-build/validation/pg17-pid-capped-kernel-20261030T175217Z");root=out/'runtime';original=json.loads((root/'matrix-result.json').read_text());build=json.loads((out/'build-result.json').read_text())
reasons={'empty':'canonical decimal value required','bare':'canonical decimal value required','leading-zero':'canonical decimal value required','zero':'canonical decimal value required','plus':'canonical decimal value required','negative':'canonical decimal value required','nondecimal':'canonical decimal value required','overflow':'value outside supported range','below301':'value outside supported range','above4096':'value outside supported range','duplicate':'duplicate boot policy','duplicate-different':'duplicate boot policy'}
results=[]
for old in original['results']:
 r=root/old['case'];raw=(r/'console.log').read_bytes();text=raw.decode(errors='replace')
 assert hashlib.sha256(raw).hexdigest()==old['consoleSha256']
 # Original parser also collected the kernel's '---[ end Kernel panic...' trailer.
 # Anchor the actual printk start line; do not alter/re-run any console/Guest evidence.
 actual=re.findall(r'^\[\s*[0-9.]+\] Kernel panic - not syncing: ([^\r\n]+)\r?$',text,re.M)
 item=dict(old);item['originalParserPass']=old['pass'];item['panicStartMessages']=actual
 if old['negativeBoot']:
  item['earlyPID0Panic']=bool(re.search(r'CPU: 0 UID: 0 PID: 0 Comm: swapper',text))
  item['earlyParamStackProven']=all(s in text for s in ['neonvm_setup_pid_max_limit+','parse_early_param+','start_kernel+'])
  item['pass']=old['intentionallyTerminatedAfterObservedPanic'] and actual==['neonvm.pid_max_limit: '+reasons[old['case']]] and item['earlyPID0Panic'] and item['earlyParamStackProven'] and not old['guestInitMarkerPresent'] and not old['kernelRunInitMarkerPresent'] and not old['probe']
 else:
  notes=re.search(r'PG17_PID_NOTES_BASE64_BEGIN\r?\n([\s\S]*?)PG17_PID_NOTES_BASE64_END',text)
  assert notes
  b=base64.b64decode(''.join(notes[1].split()),validate=True)
  item['runtimeNotesSha256']=hashlib.sha256(b).hexdigest()
  item['runtimeNotesEqualELFSection']=b==(out/'export/kernel-notes.bin').read_bytes()
  item['pass']=old['pass'] and item['runtimeNotesEqualELFSection'] and item['runtimeNotesSha256']==build['buildNotesSha256']
 if old['case']=='cap512':
  item['runtimeLSMs']=re.search(r'PG17_PID_LSM=([^\r\n]+)',text)[1]
  item['landlockProbe']=json.loads(next(l.split('=',1)[1] for l in text.splitlines() if l.startswith('PG17_LANDLOCK_PROBE={')))
  item['setTidProof']=json.loads(next(l.split('=',1)[1] for l in text.splitlines() if l.startswith('PG17_PID_SETTID={')))
  item['forkProof']=[json.loads(l.split('=',1)[1]) for l in text.splitlines() if l.startswith('PG17_PID_FORK={')]
 results.append(item)
result={'id':'PG17-S-PIDCAP-001','status':'PASS_COMPONENT_ONLY' if all(x['pass'] for x in results) else 'FAIL_COMPONENT','scope':'17 real isolated KVM boots of new PID-capped TEST_ONLY kernel','originalMatrixPath':'runtime/matrix-result.json','originalMatrixPass':original['pass'],'parserCorrection':'Original unanchored regex also collected kernel panic end trailer, causing all 12 genuine negative boots to be marked false. New evaluator anchors actual printk panic start, checks PID0/early_param call stack and no init markers. Original results/raw consoles retained; no Guest rerun.','kernel':build,'sourceOverlay':json.loads((out/'source-overlay.json').read_text()),'results':results,'counts':{'boots':len(results),'positiveBoots':sum(not x['negativeBoot'] for x in results),'negativeBoots':sum(x['negativeBoot'] for x in results),'passed':sum(x['pass'] for x in results)},'vmShrinkPermit':False,'PGStarted':False,'normalDecreaseExecuted':False,'KubernetesDeployed':False}
(out/'runtime-proof.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'status':result['status'],'counts':result['counts'],'proofSHA256':hashlib.sha256((out/'runtime-proof.json').read_bytes()).hexdigest(),'primary':next(x for x in results if x['case']=='cap512')},indent=2))
assert result['counts']['boots']==17 and result['counts']['passed']==17
