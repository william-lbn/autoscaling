#!/usr/bin/env python3
"""Development artifact helper only. It never emits a runtime Source or permit."""
from pathlib import Path
import argparse,hashlib,json,shutil,re,base64
HERE=Path(__file__).resolve().parent
TOOLS='127.0.0.1:5001/neon/ordinary-v2-tools-base@sha256:bd15f43fe3c7fc4e3c09b8cab3595ec33b25ed8a7a4d9b47ee0d23556b0a9886'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def prepare(args):
    lock=json.loads((HERE/'inputs.lock.json').read_text())
    expected={'protocol.h','creator-ipc.c','emitter-client.c','eudev-privsep-v2.patch'}
    if set(lock['revision_source_sha256'])!=expected:raise ValueError('revision source names differ')
    for name,digest in lock['revision_source_sha256'].items():
        if sha(HERE/name)!=digest:raise ValueError('locked revision source differs: '+name)
    archive=Path(args.archive);base=Path(args.base_patch);aports=Path(args.aports)
    if sha(archive)!=lock['base_eudev_archive_sha256'] or sha(base)!=lock['base_patch_sha256']:raise ValueError('original archive/base patch differs')
    for name,digest in lock['upstream_aports_sha256'].items():
        if sha(aports/name)!=digest:raise ValueError('original Alpine input differs: '+name)
    target=Path(args.context);target.mkdir(exist_ok=False)
    files={**{'revision/'+n:HERE/n for n in ['protocol.h','creator-ipc.c','emitter-client.c','eudev-privsep-v2.patch','inputs.lock.json','build-eudev.sh','Dockerfile.rebuild','test_event_properties.c']},'sources/eudev-3.2.14.tar.gz':archive,'frozen/eudev-fixed-device-v1.patch':base,**{'aports/eudev/'+n:aports/n for n in lock['upstream_aports_sha256']}}
    for name,source in files.items():dest=target/name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,dest)
    (target/'source.sha256').write_text(''.join(sha(target/name)+'  '+name+'\n' for name in sorted(files)))
    print(json.dumps({'result':'CONTEXT_ONLY_NOT_SOURCE','context':str(target),'inputs':len(files),'command':['docker','buildx','build','--builder','neon-data','--resource','memory=2g','--resource','memory-swap=2g','--resource','cpu-quota=200000','--build-arg','TOOLS_BASE='+TOOLS,'--file',str(HERE/'Dockerfile.rebuild'),str(target)]},indent=2))
def check_receipt(args):
    run=Path(args.receipt);result=json.loads((run/'result.json').read_text());console=(run/'console.log').read_text();command=json.loads((run/'qemu-command.json').read_text())
    if result['status']!='PASS_COMPONENT_ONLY' or result['Guest_exit']!=['0'] or result['QEMU_container_exit']!=0 or result['actual_kvm']!={'enabled':True,'present':True}:raise ValueError('actual run did not pass')
    if result.get('normal_authorized') is not False or result.get('PG_executed') is not False or result.get('E')!=0:raise ValueError('this model cannot qualify PG/normal/E')
    if '-accel'not in command or command[command.index('-accel')+1]!='kvm' or '-nic'not in command or command[command.index('-nic')+1]!='none':raise ValueError('isolated KVM command missing')
    match=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',console)
    notes=base64.b64decode(''.join(match[1].split()),validate=True)if match else b''
    if hashlib.sha256(notes).hexdigest()!='7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828':raise ValueError('accurate369 notes differ')
    if args.expect=='positive':
        trace=result['worker_original_pidfs_trace'];events=result['stock_public_events']
        if len(trace)!=2 or trace[0][:2]!=trace[1][:2] or [x[2]for x in trace]!=['1','2'] or events!=[x[3]for x in trace]:raise ValueError('original worker/public events differ')
        if 'PRIVSEP_NATIVE_PASS_MAIN_WORKER_STOCKLIB_MODEL_NOT_SOURCE'not in console:raise ValueError('final marker missing')
    elif args.expect=='lostack':
        if console.count('PRIVSEP_MODEL_DROP_ACK_ONCE')!=1 or console.count('PRIVSEP_MODEL_EMIT=')!=1 or 'PRIVSEP_LOST_ACK_UNKNOWN_NO_RETRY_PASS'not in console:raise ValueError('Unknown/no-retry evidence missing')
    else:
        if 'PRIVSEP_LEGACY_ROOT_STOCK_CONSUMER_PASS'not in console or len(result['stock_public_events'])!=2:raise ValueError('legacy actual consumer evidence missing')
    print(json.dumps({'result':'PASS_TEST_RECEIPT_ONLY','scope':args.expect,'receipt_sha256':sha(run/'result.json'),'Source_qualification':False,'normal_permit':False},indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(required=True)
    build=sub.add_parser('prepare');build.add_argument('--archive',required=True);build.add_argument('--base-patch',required=True);build.add_argument('--aports',required=True);build.add_argument('--context',required=True);build.set_defaults(run=prepare)
    test=sub.add_parser('check');test.add_argument('--receipt',required=True);test.add_argument('--expect',choices=['positive','lostack','legacy'],required=True);test.set_defaults(run=check_receipt)
    args=parser.parse_args();args.run(args)
