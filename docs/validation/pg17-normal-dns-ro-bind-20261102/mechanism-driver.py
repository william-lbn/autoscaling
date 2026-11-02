from pathlib import Path
import subprocess,json,os,time,hashlib,shutil
p=Path(__file__).parent;inputs=p/'inputs';root=p/'rootdisk';(root/'neonvm/bin').mkdir(parents=True);image='sha256:5d507d9bddffe506110264d45fbe7e62d6cc0e0d0e885c2226bd78866cfa42c9';start=time.monotonic()
cid=subprocess.check_output(['docker','create','--network','none',image],text=True).strip();(p/'source-container-id').write_text(cid+'\n')
try:subprocess.run(['docker','cp',cid+':/bin/busybox',str(root/'neonvm/bin/busybox')],check=True,capture_output=True)
finally:subprocess.run(['docker','rm',cid],check=True,capture_output=True)
shutil.copyfile(inputs/'original-production-hook',root/'neonvm/bin/udhcpc.script');shutil.copyfile(inputs/'inert-build-inittab',root/'neonvm/bin/inittab')
cmd=['docker','run','--rm','--network','none','--cpus','1','--memory','256m','--memory-swap','256m','--cap-drop','ALL','--cap-add','CHOWN','--security-opt','no-new-privileges:true','--entrypoint','/bin/sh','--mount','type=bind,src='+str(root)+',dst=/rootdisk','--mount','type=bind,src='+str(inputs/'prepare-root.sh')+',dst=/prepare-root.sh,readonly',image,'/prepare-root.sh','1000','1000']
r=subprocess.run(cmd,capture_output=True,text=True);(p/'prepare.stdout').write_text(r.stdout);(p/'prepare.stderr').write_text(r.stderr);(p/'prepare-command.json').write_text(json.dumps(cmd,indent=2)+'\n');assert r.returncode==0,('production prepare-root failed',r.returncode)
# 运行未改过IP/route的完整真实hook，仅隔离容器lo上设置地址；没有Host网络/Root来源。
for app in ['sh','ip','grep','head','cut','cat','chmod','mv']:
 f=root/'neonvm/bin'/app
 if not f.exists():f.symlink_to('/bin/busybox')
expected='search neon.svc.cluster.local svc.cluster.local cluster.local\nnameserver 192.0.2.53\nnameserver 192.0.2.54\n';results=[]
for name,hook,readonly in [('original-rename-control',inputs/'original-production-hook',False),('normal-inode-update',root/'neonvm/bin/udhcpc.script',False),('normal-RO-hole-reject',root/'neonvm/bin/udhcpc.script',True)]:
 f=p/(name+'.resolv');f.write_text('original-before-DHCP\n');f.chmod(0o644);before=f.stat();resolvMount='type=bind,src='+str(f)+',dst=/etc/resolv.conf'+(',readonly'if readonly else'')
 cmd=['docker','run','--rm','--read-only','--network','none','--user','0','--cpus','1','--memory','256m','--memory-swap','256m','--cap-drop','ALL','--cap-add','NET_ADMIN','--security-opt','no-new-privileges:true','--entrypoint','/bin/sh','--mount','type=bind,src='+str(root/'neonvm/bin')+',dst=/neonvm/bin,readonly','--mount','type=bind,src='+str(hook)+',dst=/hook,readonly','--mount',resolvMount,'-e','interface=lo','-e','ip=127.0.0.2','-e','mask=8','-e','subnet=255.0.0.0','-e','dns=192.0.2.53 192.0.2.54','-e','domain=neon.svc.cluster.local','-e','search=neon.svc.cluster.local svc.cluster.local cluster.local',image,'-c','if touch /etc/normal-DNS-must-not-write 2>/dev/null; then exit 91; fi; cat /proc/self/mountinfo; exec /neonvm/bin/sh /hook bound']
 r=subprocess.run(cmd,capture_output=True,text=True);(p/(name+'.stdout')).write_text(r.stdout);(p/(name+'.stderr')).write_text(r.stderr);(p/(name+'.command.json')).write_text(json.dumps(cmd,indent=2)+'\n');after=f.stat();content=f.read_text();same=(before.st_dev,before.st_ino)==(after.st_dev,after.st_ino)
 good=(r.returncode==0 and content==expected and same)if name=='normal-inode-update'else(r.returncode!=0 and content=='original-before-DHCP\n'and same)if readonly else(content=='original-before-DHCP\n'and same and 'Read-only file system'in r.stderr)
 results.append({'case':name,'exit':r.returncode,'same_original_inode':same,'DNS_written':content==expected,'pass':good,'root_readonly_assertion_failed':r.returncode==91})
(p/'result.json').write_text(json.dumps({'seconds':time.monotonic()-start,'prepare_exit':0,'actual_hook_sha256':hashlib.sha256((root/'neonvm/bin/udhcpc.script').read_bytes()).hexdigest(),'cases':results,'all_pass':all(x['pass']for x in results),'classification':'actual isolated Linux filesystem/DHCP hook mechanism; no PG/Guest18c/Source/normal budget/K8s','host_network_changed':False,'Source':False,'normal':False,'K8s':False,'E':0},indent=2)+'\n');print(json.dumps(results),flush=True);raise SystemExit(0 if all(x['pass']for x in results)else 1)
