from pathlib import Path
import hashlib,json
r=Path('/build/linux-6.12.26');o=Path('/force-fs-leaf');o.mkdir();sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();ip=Path('/force-review/source-index.json');assert sha(ip)=='273af74cfed5f69b98b189a4c12e95128af0a9e5605bda728868c9e2ac4833b1';index=json.loads(ip.read_text());selected=['fs/ext4/extents.c','fs/ext4/page-io.c','fs/jbd2/journal.c','fs/jbd2/revoke.c','include/linux/buffer_head.h','kernel/fork.c','kernel/sched/core.c','mm/Makefile','fs/ext4/Makefile','net/sunrpc/Makefile','drivers/block/Makefile','drivers/net/Makefile','drivers/scsi/Makefile','drivers/nvme/host/Makefile']
result={}
for n in selected:
 p=o/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((r/n).read_bytes());h=sha(p)
 if n in index:assert h==index[n]
 result[n]={'sha256':h,'in_original_source_index':n in index}
(o/'result.json').write_text(json.dumps({'selected_files':result,'parent_index_sha256':sha(ip),'U':0,'S':0,'normal_authorized':False,'E':0},indent=2)+'\n')
