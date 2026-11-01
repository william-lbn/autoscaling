from pathlib import Path
import hashlib,json,re,subprocess
r=Path('/build/linux-6.12.26');o=Path('/force-review');o.mkdir();sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
expected={'arch/x86/boot/bzImage':'18c9fd3a38bbe5a087b98998c856898e97ee3f0bc68681b13f0287912849e1ad','.config':'966e615127358d6d0dcf4f6ca884a07702e957ad07a95909cf95feca4aae5441'}
for n,h in expected.items():assert sha(r/n)==h,n
subprocess.run(['objcopy','--dump-section','.notes='+str(o/'kernel-notes.bin'),str(r/'vmlinux')],check=True)
assert sha(o/'kernel-notes.bin')=='a496a3c44fc0b374f100ba0331a2e21f02d6e79ac999769a8d3ad4d71b76024e'
symbols=['mem_cgroup_replace_folio','replace_page_cache_folio','memalloc_noreclaim_save','memalloc_noreclaim_restore','mem_cgroup_charge','page_counter_charge','alloc_migrate_folio','alloc_migration_target','alloc_misplaced_dst_folio','numa_demotion_enabled','put_pages_list','free_unref_folios','mem_cgroup_uncharge']
patterns={s:re.compile(r'\b'+re.escape(s)+r'\b')for s in symbols};hits={s:[]for s in symbols};index={}
for p in sorted(r.rglob('*')):
 if not p.is_file()or p.suffix not in ['.c','.h','.S','.rs']:continue
 name=str(p.relative_to(r));raw=p.read_bytes();index[name]=hashlib.sha256(raw).hexdigest()
 text=raw.decode(errors='replace')
 for s,pattern in patterns.items():
  if s not in text:continue
  for line,value in enumerate(text.splitlines(),1):
   if pattern.search(value):hits[s].append({'path':name,'line':line,'text':value})
selected=['mm/memcontrol.c','mm/page_alloc.c','mm/vmscan.c','mm/swap.c','mm/filemap.c','mm/shmem.c','mm/migrate.c','mm/mempolicy.c','mm/memory.c','mm/compaction.c','mm/gup.c','mm/memory_hotplug.c','mm/memory-failure.c','mm/memory-tiers.c','net/core/sock.c','fs/ext4/inode.c','fs/ext4/mballoc.c','fs/ext4/super.c','fs/ext4/xattr.c','fs/ext4/inline.c','fs/ext4/extents_status.c','fs/jbd2/transaction.c','block/bdev.c','fs/buffer.c','fs/fuse/dev.c','include/linux/gfp.h','include/linux/gfp_types.h','mm/internal.h','include/linux/page_counter.h','mm/page_counter.c','arch/x86/entry/entry_64.S','arch/x86/entry/common.c','arch/x86/mm/fault.c','arch/x86/include/asm/irq_stack.h']
for name in selected:
 p=r/name;assert p.is_file(),name;q=o/'source'/name;q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes(p.read_bytes())
(o/'source-index.json').write_text(json.dumps(index,sort_keys=True,indent=2)+'\n');(o/'symbol-occurrences.json').write_text(json.dumps(hits,sort_keys=True,indent=2)+'\n')
config={}
for line in(r/'.config').read_text().splitlines():
 if line.startswith('CONFIG_')and'='in line:k,value=line.split('=',1);config[k]=value
 elif line.startswith('# CONFIG_')and line.endswith(' is not set'):config[line[2:-11]]='n'
(o/'resolved-config.json').write_text(json.dumps(config,sort_keys=True,indent=2)+'\n')
report={'status':'EXPORTED_FOR_REVIEW_NOT_PROOF','source_files_indexed':len(index),'selected_files':{n:sha(r/n)for n in selected},'symbol_occurrences':{s:len(x)for s,x in hits.items()},'matching_strategy':'whole identifier references incl comments/definitions, full .c/.h/.S/.rs tree incl noncompiled code; human classify compiled/reachable edges, not a compiler callgraph or automatic absence proof','actual_tuple':expected,'source_index_sha256':sha(o/'source-index.json'),'symbol_occurrences_sha256':sha(o/'symbol-occurrences.json'),'kernel_notes_sha256':sha(o/'kernel-notes.bin'),'kernel_release':(r/'include/config/kernel.release').read_text().strip(),'U':0,'S':0,'normal_authorized':False,'E':0}
(o/'result.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
