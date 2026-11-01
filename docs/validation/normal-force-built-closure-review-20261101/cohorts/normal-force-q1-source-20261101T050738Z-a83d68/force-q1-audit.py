from pathlib import Path
import hashlib,json,re,subprocess
r=Path('/build/linux-6.12.26');o=Path('/force-q1');o.mkdir();sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();index_path=Path('/force-review/source-index.json');assert sha(index_path)=='273af74cfed5f69b98b189a4c12e95128af0a9e5605bda728868c9e2ac4833b1';index=json.loads(index_path.read_text());names=sorted(index)
patterns=['SOCK_MEMALLOC','sk_memalloc_socks','mapping_set_large_folios','mapping_set_folio_order_range','mapping_min_folio_order','shmem_huge_enabled','shmem_allowable_huge_orders','shmem_get_orders_within_size']
regex=r'\b('+ '|'.join(patterns)+r')\b';commands=[];raw=bytearray()
for start in range(0,len(names),700):
 cmd=['grep','-nHE',regex,'--',*names[start:start+700]];x=subprocess.run(cmd,cwd=r,stdout=subprocess.PIPE,stderr=subprocess.PIPE);assert x.returncode in [0,1],x.stderr.decode();raw.extend(x.stdout);commands.append({'argv':cmd,'exit':x.returncode,'stderr':x.stderr.decode()})
(o/'grep.stdout').write_bytes(raw);(o/'grep-commands.json').write_text(json.dumps(commands,indent=2)+'\n')
hits={k:[]for k in patterns};selected=set(['mm/huge_memory.c','include/linux/huge_mm.h','mm/shmem.c','mm/filemap.c','include/linux/pagemap.h','net/core/sock.c','net/core/dev.c','kernel/softirq.c','include/linux/sched/mm.h','include/net/sock.h','include/uapi/asm-generic/socket.h','mm/swap_state.c','mm/folio-compat.c'])
for line in raw.decode().splitlines():
 name,line_no,text=line.split(':',2);line_no=int(line_no);selected.add(name)
 for k in set(re.findall(regex,text)):hits[k].append({'path':name,'line':line_no,'text':text})
for path in ['/force-review/symbol-occurrences.json','/force-extra/symbol-occurrences.json']:
 prior=json.loads(Path(path).read_text())
 for k in ['PF_MEMALLOC','sk_set_memalloc','sk_clear_memalloc','memalloc_noreclaim_save','memalloc_noreclaim_restore']:
  for h in prior.get(k,[]):selected.add(h['path'])
exported={}
for name in sorted(selected):
 if name not in index:continue
 q=o/'source'/name;q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes((r/name).read_bytes());assert sha(q)==index[name],name;exported[name]=index[name]
(o/'occurrences.json').write_text(json.dumps(hits,indent=2)+'\n')
result={'status':'READONLY_SELECTED_SOURCE_NOT_REACHABILITY_PROOF','parent_index_sha256':sha(index_path),'all_tree_indexed_files':len(index),'GNU_grep_batches':len(commands),'source_hash_verified_only_selected':len(exported),'selected_files':exported,'symbol_counts':{k:len(x)for k,x in hits.items()},'grep_stdout_sha256':sha(o/'grep.stdout'),'grep_commands_sha256':sha(o/'grep-commands.json'),'U':0,'S':0,'normal_authorized':False,'E':0};(o/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
