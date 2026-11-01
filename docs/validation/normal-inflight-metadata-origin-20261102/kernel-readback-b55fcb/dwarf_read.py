import subprocess,re,json,sys,os
from pathlib import Path
elf=sys.argv[1];dest=Path(sys.argv[2]);cmd=json.loads(os.environ.get('READELF_PREFIX_JSON','[\"readelf\"]'))+['--debug-dump=info','--dwarf-depth=2',elf];q=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);wanted={'pt_regs','exception_stacks','entry_stack_page'};found={k:[]for k in wanted};counts={k:0 for k in wanted};sizes_seen={k:set() for k in wanted};block=[]
def flush():
 if not block:return
 h=block[0]
 if 'DW_TAG_structure_type'not in h:return
 name=None;size=None
 parent=[]
 for l in block:
  m=re.match(r'\s*<(\d+)><[0-9a-f]+>:',l)
  if m and int(m.group(1))>1:break
  parent.append(l)
 if any('DW_AT_declaration' in l for l in parent):return
 for l in parent:
  if 'DW_AT_name'in l:
   for n in wanted:
    if l.rstrip().endswith(': '+n):name=n
  if 'DW_AT_byte_size'in l:
   z=l.rsplit(':',1)[-1].strip()
   try:size=int(z,0)
   except ValueError:pass
 if name and size is not None:
  counts[name]+=1;sizes_seen[name].add(size)
  if len(found[name])<2:found[name].append({'byte_size':size,'raw_DIE_with_member_offsets':''.join(block)})
for line in q.stdout:
 m=re.match(r'\s*<(\d+)><[0-9a-f]+>:',line)
 if m and int(m.group(1))<=1:flush();block=[]
 block.append(line)
flush();err=q.stderr.read();rc=q.wait();sizes={n:sorted(xs)for n,xs in sizes_seen.items()};summary={'command':cmd,'exit':rc,'stderr':err,'types':found,'complete_definition_counts':counts,'all_complete_definition_sizes':sizes};dest.write_text(json.dumps(summary,indent=2)+'\n');assert rc==0
print(json.dumps(sizes));assert sizes['pt_regs']==[168];assert sizes['exception_stacks']==[32768];assert len(sizes['entry_stack_page'])==1
