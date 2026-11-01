from pathlib import Path
import hashlib,json
r=Path('/build/linux-6.12.26');o=Path('/force-header');o.mkdir();sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();ip=Path('/force-review/source-index.json');assert sha(ip)=='273af74cfed5f69b98b189a4c12e95128af0a9e5605bda728868c9e2ac4833b1';index=json.loads(ip.read_text());selected=['include/linux/fscrypt.h','arch/x86/include/asm/page_types.h','kernel/sched/debug.c','include/linux/pagevec.h']
for n in selected:
 p=o/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((r/n).read_bytes());assert sha(p)==index[n]
(o/'result.json').write_text(json.dumps({'selected_files':{n:index[n]for n in selected},'parent_index_sha256':sha(ip),'U':0,'S':0,'normal_authorized':False,'E':0},indent=2)+'\n')
