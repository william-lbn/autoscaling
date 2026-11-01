from pathlib import Path
import gzip, tarfile, io, json, hashlib, shutil, re, os, sys

job=Path(sys.argv[1]);out=job/'provider-fixture';out.mkdir();root=out/'root';root.mkdir()
repo=Path('/data/neon-build/src/autoscaling')
research=Path('/data/neon-build/validation/ordinary-services-v2-research-20261101T044713Z-c6ff82')
runtime=Path('/data/neon-build/validation/ordinary-v2-packages-build-20261101T060652Z-8d5217/runtime-apks')
patched=Path('/data/neon-build/validation/ordinary-udev-privsep-build-20261101T080238Z-e47368')
sha=lambda b:hashlib.sha256(b).hexdigest()
assert sha((patched/'source-lock.json').read_bytes())=='4c585dbb7c7e86b97ecfbc221ee40af86c2d8785865463a56dacc71d2fbac8f8'
inputs={'privsep-build/source-lock.json':sha((patched/'source-lock.json').read_bytes())}
for name,h in json.loads((patched/'source-lock.json').read_bytes())['source_inputs'].items():inputs['privsep-build/'+name]=h
members={};apk_facts=[]
for source in [research/'apks',runtime]:
 for apk in sorted(source.glob('*.apk')):
  key='apk/'+apk.name;data=apk.read_bytes();digest=sha(data)
  if key in inputs:
   assert inputs[key]==digest,(key,'same APK name changed');continue
  inputs[key]=digest;found={}
  with tarfile.open(fileobj=io.BytesIO(gzip.decompress(data)),mode='r:',ignore_zeros=True)as tf:
   for m in tf:
    name=m.name.removeprefix('./')
    if name.startswith('/')or '..'in Path(name).parts:raise ValueError('unsafe original APK member')
    if m.isfile():
     f=tf.extractfile(m);raw=f.read();assert len(raw)==m.size
     found[name]=(raw,m.mode,key)
  members[key]=found
  apk_facts.append({'path':str(apk),'sha256':digest,'regular_members':len(found)})
needed=[]
inv=(repo/'pkg/dsga/resourceguard/normal_ordinary_stock_udev_inventory.go').read_text()
needed+=['/lib/udev/rules.d/'+n for n in re.findall(r'"([^"]+\.rules)"',inv)if '/'not in n]
needed+=['/etc/udev/hwdb.d/'+n for n in re.findall(r'"([^"]+\.hwdb)"',inv)]
assert len(needed)==55,(len(needed),needed)
needed+=['/lib/udev/'+n for n in ['ata_id','cdrom_id','collect','dmi_memory_id','fido_id','mtd_probe','scsi_id','v4l_id']]
needed+=['/etc/udev/udev.conf']
files=[];provenance={}
def put(logical,raw,mode,input_key,member):
 path=root/logical.lstrip('/');path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw);os.chmod(path,mode)
 files.append({'path':logical,'bytes':str(len(raw)),'sha256':sha(raw),'input':input_key,'member':member})
 provenance[logical]={'input':input_key,'input_sha256':inputs[input_key],'member':member,'bytes':len(raw),'sha256':sha(raw)}
for logical in needed:
 choices=[]
 for key,archive in members.items():
  for name in [logical.lstrip('/'),('usr'+logical).lstrip('/')]:
   if name in archive:choices.append((key,name,archive[name]))
 # Existing signed runtime/research APK names deduplicate; equal content duplicates are allowed only here.
 assert choices,('required original member missing',logical)
 assert len({sha(x[2][0])for x in choices})==1,('ambiguous APK providers',logical)
 key,name,(raw,mode,_)=choices[0];put(logical,raw,mode,key,name)
for logical,member in [('/neonvm/bin/udevd','sbin/udevd'),('/neonvm/bin/udevadm','bin/udevadm')]:
 p=patched/'export/eudev-root'/member;put(logical,p.read_bytes(),p.stat().st_mode&0o777,'privsep-build/source-lock.json','export/eudev-root/'+member)
# Exact DSOs expected by this audited provider family; the builder independently follows actual DT_NEEDED.
aliases={
 '/lib/ld-musl-x86_64.so.1':'lib/ld-musl-x86_64.so.1',
 '/lib/libc.musl-x86_64.so.1':'lib/ld-musl-x86_64.so.1',
 '/lib/libblkid.so.1':'lib/libblkid.so.1.1.0',
 '/lib/libkmod.so.2':'lib/libkmod.so.2.4.1',
 '/lib/libz.so.1':'lib/libz.so.1.3.1',
 '/usr/lib/liblzma.so.5':'usr/lib/liblzma.so.5.4.5',
 '/usr/lib/libzstd.so.1':'usr/lib/libzstd.so.1.5.5',
 '/lib/libcrypto.so.3':'lib/libcrypto.so.3',
 '/lib/libudev.so.1':'lib/libudev.so.1.6.3',
}
for logical,member in aliases.items():
 choices=[(key,archive[member])for key,archive in members.items()if member in archive]
 assert choices,('DSO missing',logical,member)
 assert len({sha(x[1][0])for x in choices})==1
 key,(raw,mode,_)=choices[0];put(logical,raw,mode,key,member)
for logical,data,mode in [
 ('/etc/passwd',b'root:x:0:0:root:/root:/bin/sh\npostgres:x:1000:1000:postgres:/var/db/postgres:/bin/sh\n',0o644),
 ('/etc/group',b'root:x:0:\npostgres:x:1000:\n',0o644),
 ('/etc/resolv.conf',b'nameserver 192.0.2.53\n',0o644),
 ('/lib/udev/rules.d/99-virtio-ports.rules',b'SUBSYSTEM=="virtio-ports", KERNEL=="vport*", ATTR{name}=="?*", SYMLINK+="virtio-ports/$attr{name}"\n',0o644),
]:
 p=root/logical.lstrip('/');p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data);os.chmod(p,mode)
for logical in ['etc/udev/rules.d','run','var/log','var/lib/chrony','neonvm/config']:(root/logical).mkdir(parents=True,exist_ok=True)
lock={'protocol_version':1,'scope':'normal-stock-udev-rootdisk-inputs-v1','inputs':inputs,'files':sorted(files,key=lambda f:f['path'])}
(out/'package-lock.json').write_text(json.dumps(lock,sort_keys=True,indent=2)+'\n')
(out/'fixture-provenance.json').write_text(json.dumps({'scope':'ACTUAL_APK_AND_PATCHED_PROVIDER_STATIC_ASSET_FIXTURE_NOT_FULL_SYSTEM_OR_SOURCE','original_apks':apk_facts,'patched_provider_source_lock':str(patched/'source-lock.json'),'patched_provider_source_lock_sha256':inputs['privsep-build/source-lock.json'],'all_files':provenance,'accounts_and_DNS':'explicit local rootdisk fixture, not a full production image','original_signed_APK_verification':str(research/'apk-verify.json'),'original_signed_APK_verification_sha256':sha((research/'apk-verify.json').read_bytes())},indent=2,sort_keys=True)+'\n')
print(json.dumps({'fixture':str(out),'actual_provider_files':len(files),'APK_inputs':len(apk_facts),'package_lock_sha256':sha((out/'package-lock.json').read_bytes()),'normal':False}))
