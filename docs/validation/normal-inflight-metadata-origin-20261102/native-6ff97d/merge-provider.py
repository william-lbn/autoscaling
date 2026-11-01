from pathlib import Path
import json,hashlib,os,posixpath,shutil
import sys
root=Path(sys.argv[1]);provider=Path(sys.argv[2]);proof=Path(sys.argv[3]);proof.mkdir(parents=True,exist_ok=True)
raw=(provider/'package-lock.json').read_bytes();lock=json.loads(raw)
def virtual(logical):
 if not logical.startswith('/') or posixpath.normpath(logical)!=logical:raise RuntimeError('unclean image path')
 parts=logical.strip('/').split('/');done=[];hops=0
 while parts:
  part=parts.pop(0)
  if part in ['', '.']:continue
  if part=='..':
   if not done:raise RuntimeError('image root escape')
   done.pop();continue
  candidate=root.joinpath(*done,part)
  if candidate.is_symlink():
   hops+=1
   if hops>40:raise RuntimeError('image alias loop')
   target=os.readlink(candidate)
   if target.startswith('/'):done=[]
   parts=target.strip('/').split('/')+parts
  else:done.append(part)
 return root.joinpath(*done)
merged=[]
for f in lock['files']:
 logical=f['path'];source=provider/'root'/logical[1:]
 if not source.is_file()or source.is_symlink():raise RuntimeError('provider must contain locked regular file '+logical)
 b=source.read_bytes()
 if hashlib.sha256(b).hexdigest()!=f['sha256']or str(len(b))!=str(f['bytes']):raise RuntimeError('provider bytes mismatch '+logical)
 dest=virtual(logical);dest.parent.mkdir(parents=True,exist_ok=True)
 before=hashlib.sha256(dest.read_bytes()).hexdigest()if dest.is_file()else None
 dest.write_bytes(b);dest.chmod(source.stat().st_mode & 0o777)
 merged.append({'logical':logical,'canonical':'/'+str(dest.relative_to(root)),'source':str(source),'sha256':f['sha256'],'before_sha256':before})
# Existing actual lookup aliases are retained. Rebind only logical DSO path when the
# exact original APK/build member bytes already win at the earlier default directory.
deriv=[]
for f in lock['files']:
 name=posixpath.basename(f['path'])
 if not '.so' in name and not name.startswith('ld-musl'):continue
 selected=None
 for directory in ['/lib','/usr/local/lib','/usr/lib']:
  logical=directory+'/'+name;dest=virtual(logical)
  if dest.is_file():selected=(logical,dest);break
 if selected is None:raise RuntimeError('missing actual needed lookup '+name)
 logical,dest=selected;actual=hashlib.sha256(dest.read_bytes()).hexdigest()
 if actual!=f['sha256']or dest.stat().st_size!=int(f['bytes']):raise RuntimeError('earlier existing alias has different unproven member '+logical)
 if logical!=f['path']:
  deriv.append({'original_logical':f['path'],'actual_selected_logical':logical,'canonical':'/'+str(dest.relative_to(root)),'sha256':actual,'retained_input':f['input'],'retained_member':f['member']});f['path']=logical
lock['files'].sort(key=lambda x:x['path'])
if len({f['path']for f in lock['files']})!=len(lock['files']):raise RuntimeError('derived lock duplicate')
rawnew=json.dumps(lock,sort_keys=True,separators=(',',':')).encode()+b'\n';(proof/'package-lock-derived.json').write_bytes(rawnew)
rule=b'SUBSYSTEM=="virtio-ports", KERNEL=="vport*", ATTR{name}=="?*", SYMLINK+="virtio-ports/$attr{name}"\n'
assert len(rule)==99
rulepath=virtual('/lib/udev/rules.d/99-virtio-ports.rules');rulepath.parent.mkdir(parents=True,exist_ok=True);oldrule=rulepath.read_bytes()if rulepath.exists()else None
if oldrule is not None and oldrule!=rule:raise RuntimeError('existing virtio rule differs; do not replace unknown rule')
rulepath.write_bytes(rule);rulepath.chmod(0o644)
(proof/'merge-source-proof.json').write_text(json.dumps({'scope':'disposable root pre-RO final build; no runtime authority','original_lock_sha256':hashlib.sha256(raw).hexdigest(),'derived_lock_sha256':hashlib.sha256(rawnew).hexdigest(),'files':merged,'lookup_derivation':deriv,'virtio_rule_sha256':hashlib.sha256(rule).hexdigest(),'aliases_removed':False},indent=2)+'\n')
