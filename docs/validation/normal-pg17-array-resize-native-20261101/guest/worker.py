from pathlib import Path
import subprocess,json,hashlib,shutil,os,re
out=Path(__file__).parent;go=Path(__import__('sys').argv[1]);v=Path('/data/neon-build/validation')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
old=v/'array-service-native-20261031T221332Z-f8811b';root=out/'root'
shutil.copytree(old/'root',root,symlinks=True)
shutil.copyfile(go/'export/array-resize.test',root/'neonvm/bin/resource-guard');(root/'neonvm/bin/resource-guard').chmod(0o4755)
shutil.copyfile(go/'export/fixture/challenge.json',root/'fixture/challenge.json')
shutil.copyfile(go/'export/fixture/required-kernel-args.txt',out/'required-kernel-args.txt')
shutil.copytree(v/'array-service-native-20261031T212646Z-fc63e3/usr-tree',out/'usr-tree',symlinks=True)
usr=out/'usr-tree';shutil.copyfile(go/'export/fixture/native-manifest.json',usr/'share/neon-dsga/native-build.json')
image='127.0.0.1:5001/neon/compute-v17@sha256:47ef74c7bee6018c1e8578486d1196bd0c9b6d84040f6ccdba97823dcd019e55'
container=subprocess.check_output(['docker','create','--network','none','--entrypoint','/bin/true',image],text=True).strip()
closure={};commands=[];loaderlogs={}
def copy(path):
 if path.startswith('/lib/x86_64-linux-gnu/'):relative='lib/x86_64-linux-gnu/'+Path(path).name
 elif path.startswith('/usr/'):relative=path[len('/usr/'):]
 else:raise ValueError('unreviewed runtime path '+path)
 dest=usr/relative;dest.parent.mkdir(parents=True,exist_ok=True)
 cmd=['docker','cp','-L',container+':'+path,str(dest)];commands.append(cmd)
 # 不用docker cp覆盖已存在symlink，使实际闭包对象/历史16仍保持唯一字节。
 tmp=dest.with_name(dest.name+'.runtime-copy-tmp')
 c=cmd[:-1]+[str(tmp)];r=subprocess.run(c,capture_output=True);assert r.returncode==0,r.stderr.decode()
 if dest.exists():
  assert sha(dest)==sha(tmp),(path,'existing ELF bytes changed');tmp.unlink()
 else:tmp.rename(dest)
 closure[path]={'guest_path':'/usr/'+relative,'sha256':sha(dest),'bytes':dest.stat().st_size}
try:
 for elf in ['/usr/local/lib/neon.so','/usr/local/bin/psql']:
  cmd=['docker','run','--rm','--pull=never','--network=none','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges','--cpus=1','--memory=256m','--memory-swap=256m','--entrypoint','/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2',image,'--list',elf]
  commands.append(cmd);run=subprocess.run(cmd,capture_output=True,text=True);assert run.returncode==0,run.stderr;loaderlogs[elf]=run.stdout
  copy(elf)
  deps=set(re.findall(r'=>\s+(/[^\s]+)',run.stdout))
  deps.update(re.findall(r'^\s*(/[^\s]+)\s+\(',run.stdout,re.M))
  for path in sorted(deps):copy(path)
finally:subprocess.run(['docker','rm',container],capture_output=True,check=True)
assert sha(usr/'local/lib/neon.so')=='1e9468b50f065cf76eb82eb5cd35bc83e148d4a5cd50211ea5931069ffdc03e3'
assert sha(usr/'local/bin/postgres')=='9f3b8eafeddcc37107e39158d1b6e30ad959bb385498db65526dc7436d2be9a7'
(out/'runtime-plugin-closure.json').write_text(json.dumps({'scope':'TEST_COMPONENT_RUNTIME_FILES_NOT_PREMAIN_SOURCE_MANIFEST','image':image,'files':closure,'commands':commands,'actual_default_loader_list':loaderlogs,'test_dsg_region_used':False,'other_extension_plugins_enabled':False,'normal_source_plugin_manifest_still_unclosed':True},indent=2)+'\n')
with(out/'usr.ext4').open('wb')as f:f.truncate(512<<20)
cmd=['mkfs.ext4','-q','-F','-d',str(usr),str(out/'usr.ext4')];(out/'mkfs-usr-command.json').write_text(json.dumps(cmd,indent=2)+'\n');subprocess.run(cmd,check=True)
with(out/'state.ext4').open('wb')as f:f.truncate(64<<20)
subprocess.run(['mkfs.ext4','-q','-F',str(out/'state.ext4')],check=True)
shutil.copytree(go/'export/fixture/isofiles',out/'isofiles')
cmd=['docker','run','--rm','--network=none','--pull=never','--cpus=1','--memory=256m','--memory-swap=256m','--entrypoint','genisoimage','-v',str(out)+':/work:rw','neon-test-only/dsga-kernel-bootstrap-iso:20261031','-quiet','-R','-J','-o','/work/runtime.iso','/work/isofiles']
x=subprocess.run(cmd,capture_output=True,text=True);(out/'iso-build.json').write_text(json.dumps({'command':cmd,'exit':x.returncode,'stdout':x.stdout,'stderr':x.stderr},indent=2)+'\n');assert x.returncode==0
p=root/'init';s=p.read_text().replace('TestNativeArrayServiceBirthAndPopulate','TestNativeNormalPG17ArrayGrowShrink').replace('-test.timeout=110s','-test.timeout=140s')
a=s.index('for file in /evidence/*; do');b=s.index('\nsync\npoweroff',a)
s=s[:a]+r'''for file in /evidence/*; do
 test -f "$file" || continue
 name=$(basename "$file")
 hash=$(sha256sum "$file" | cut -d' ' -f1)
 size=$(wc -c < "$file" | tr -d ' ')
 echo "PG17_ARRAY_REPORT_BEGIN=$name:$size:$hash"
 blocks=$(( (size + 767) / 768 ))
 i=0
 while test "$i" -lt "$blocks"; do
  blob=$(dd if="$file" bs=768 skip="$i" count=1 2>/dev/null | base64 | tr -d '\n')
  echo "PG17_ARRAY_REPORT_CHUNK=$name:$i:$blob"
  i=$((i + 1))
 done
 echo "PG17_ARRAY_REPORT_END=$name"
done
'''+s[b:];p.write_text(s);p.chmod(0o755)
files=[]
for p in[root]+sorted(root.rglob('*')):
 st=p.lstat();files.append({'path':str(p.relative_to(root)),'mode':oct(st.st_mode),'symlink':os.readlink(p)if p.is_symlink()else None,'sha256':sha(p)if p.is_file()and not p.is_symlink()else None})
a=subprocess.Popen(['find','.','-print0'],cwd=root,stdout=subprocess.PIPE);b=subprocess.Popen(['cpio','--null','-o','--format=newc'],cwd=root,stdin=a.stdout,stdout=subprocess.PIPE,stderr=subprocess.PIPE);a.stdout.close()
with(out/'initramfs.cpio.gz').open('wb')as f:c=subprocess.run(['gzip','-n','-9'],stdin=b.stdout,stdout=f);b.stdout.close()
assert c.returncode==0 and b.wait()==a.wait()==0
(out/'initramfs-manifest.json').write_text(json.dumps({'files':files,'archive_sha256':sha(out/'initramfs.cpio.gz')},indent=2)+'\n')
(out/'source-lock.json').write_text(json.dumps({'base':'f553f074f3f745689eb3145bf8cef8037b6e22ea','Go_build':str(go),'Go_source_lock_sha256':sha(go/'source-lock.json'),'Go_ELF_sha256':sha(go/'export/array-resize.test'),'PG_ELF_sha256':sha(usr/'local/bin/postgres'),'neon_so_sha256':sha(usr/'local/lib/neon.so'),'helper_sha256':sha(root/'neonvm/bin/neon-dsga-array-populate'),'pre_main_manifest_sha256':sha(usr/'share/neon-dsga/native-build.json'),'runtime_plugin_closure_sha256':sha(out/'runtime-plugin-closure.json'),'runtime_ISO_sha256':sha(out/'runtime.iso'),'usr_ext4_sha256':sha(out/'usr.ext4'),'new_test_only_source':json.loads((go/'source-lock.json').read_text())['owned'],'Host_Source_is_fixture':True,'issuer_is_fixture':True,'journal_StartCAS_covered':False,'normal_authorized':False,'E':0},indent=2)+'\n')
runner=(old/'run-kvm.py').read_text().replace('TestNativeArrayServiceBirthAndPopulate','TestNativeNormalPG17ArrayGrowShrink').replace('PG17-S-ARRAY-SERVICE-001','PG17-S-ARRAY-RESIZE-001').replace('array-service-real.json','normal-pg17-array-resize.json').replace("'150s'","'180s'")
where="roots=re.findall("
idx=runner.index(where)
parser=r'''for n,size,h in re.findall(r'^PG17_ARRAY_REPORT_BEGIN=([^:\r\n]+):(\d+):([a-f0-9]{64})\r?$',text,re.M):
 try:
  assert n==Path(n).name and n not in records and int(size)<=1024*1024
  chunks=re.findall(r'^PG17_ARRAY_REPORT_CHUNK='+re.escape(n)+r':(\d+):([A-Za-z0-9+/=]+)\r?$',text,re.M)
  assert [int(i)for i,_ in chunks]==list(range((int(size)+767)//768))
  assert len(re.findall(r'^PG17_ARRAY_REPORT_END='+re.escape(n)+r'\r?$',text,re.M))==1
  raw=b''.join(base64.b64decode(blob,validate=True)for _,blob in chunks)
  assert len(raw)==int(size)and hashlib.sha256(raw).hexdigest()==h
  records[n]=json.loads(raw)if n.endswith('.json')else{'text_bytes':len(raw)}
  (out/'reports'/n).write_bytes(raw)
 except Exception as e:errors.append('chunk '+n+':'+repr(e))
'''
runner=runner[:idx]+parser+runner[idx:]
(out/'run-kvm.py').write_text(runner)
subprocess.run(['python3',str(out/'run-kvm.py')],check=True)
