from pathlib import Path
import hashlib,json,subprocess
r=Path('/build/linux-6.12.26');o=Path('/force-key');o.mkdir();sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();ip=Path('/force-review/source-index.json');assert sha(ip)=='273af74cfed5f69b98b189a4c12e95128af0a9e5605bda728868c9e2ac4833b1';index=json.loads(ip.read_text());names=sorted(index);commands=[];raw=bytearray()
for start in range(0,len(names),700):
 cmd=['grep','-nHE',r'\b(memalloc_socks_key|SO_MEMALLOC)\b','--',*names[start:start+700]];x=subprocess.run(cmd,cwd=r,stdout=subprocess.PIPE,stderr=subprocess.PIPE);assert x.returncode in [0,1];raw.extend(x.stdout);commands.append({'argv':cmd,'exit':x.returncode,'stderr':x.stderr.decode()})
(o/'grep.stdout').write_bytes(raw);(o/'grep-commands.json').write_text(json.dumps(commands,indent=2)+'\n');selected=sorted(set(l.split(':',1)[0]for l in raw.decode().splitlines()))
for name in selected:
 q=o/'source'/name;q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes((r/name).read_bytes());assert sha(q)==index[name]
(o/'result.json').write_text(json.dumps({'selected_files':{n:index[n]for n in selected},'parent_index_sha256':sha(ip),'all_tree_files':len(index),'GNU_grep_batches':len(commands),'grep_stdout_sha256':sha(o/'grep.stdout'),'grep_commands_sha256':sha(o/'grep-commands.json'),'U':0,'S':0,'normal_authorized':False,'E':0},indent=2)+'\n')
