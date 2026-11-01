from pathlib import Path
import json,re,hashlib
r=Path('/build/linux-6.12.26');o=Path('/force-extra');o.mkdir();sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
index=json.loads(Path('/force-review/source-index.json').read_text());assert sha(Path('/force-review/source-index.json'))=='273af74cfed5f69b98b189a4c12e95128af0a9e5605bda728868c9e2ac4833b1'
symbols=['PF_MEMALLOC','sk_set_memalloc','sk_clear_memalloc','__GFP_NOFAIL','__GFP_HIGH','__mem_cgroup_charge'];pattern=re.compile(r'\b('+ '|'.join(map(re.escape,symbols))+r')\b');hits={s:[]for s in symbols}
for name,h in index.items():
 b=(r/name).read_bytes()
 if not any(s.encode()in b for s in symbols):continue
 assert hashlib.sha256(b).hexdigest()==h,name
 lines=b.decode(errors='replace').splitlines()
 for line,text in enumerate(lines,1):
  for symbol in set(pattern.findall(text)):
   hits[symbol].append({'path':name,'line':line,'text':text,'context':lines[max(0,line-5):min(len(lines),line+4)]})
selected=['net/core/dev.c','net/core/sock.c','include/linux/sched/mm.h','include/uapi/asm-generic/socket.h','include/net/sock.h','include/linux/swap.h','include/linux/pagemap.h','include/linux/memcontrol.h']
for name in selected:
 q=o/'source'/name;q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes((r/name).read_bytes());assert sha(q)==index[name]
(o/'symbol-occurrences.json').write_text(json.dumps(hits,indent=2)+'\n')
report={'status':'EXPORTED_FOR_MANUAL_CLASSIFICATION_NOT_PROOF','indexed_source_count':len(index),'parent_index_sha256':'273af74cfed5f69b98b189a4c12e95128af0a9e5605bda728868c9e2ac4833b1','symbol_occurrences':{s:len(x)for s,x in hits.items()},'selected_files':{n:index[n]for n in selected},'symbol_occurrences_sha256':sha(o/'symbol-occurrences.json'),'scope':'exact cached built18c source; comments/definitions/noncompiled files included; no automatic reachable-callgraph claim','U':0,'S':0,'normal_authorized':False}
(o/'result.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
