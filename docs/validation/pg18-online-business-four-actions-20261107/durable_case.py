"""PG18 同一普通应用连接持续提交。由已部署 Agent 按真实 SQL 压力发起内存操作。"""
import base64
import concurrent.futures
import datetime
import hashlib
import ipaddress
import json
import os
import re
import select
import subprocess
import time
import urllib.request
import uuid
import zlib

VM = 'cp-24c41e23bb104ead'
SCHEMA = 'dsga_pg18_wal_02679a05'
ORDER = ('grow','shrink','grow','shrink')
CHECKSUM = hashlib.md5('|'.join(f"{i}:"+hashlib.md5(f"pg18-wal-{i}".encode()).hexdigest() for i in range(1,513)).encode()).hexdigest()
JOURNAL = 'internal.autoscaling.neon.tech/dsga-journal-v1'
HOST = 'internal.autoscaling.neon.tech/dsga-host-ledger-v1'

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def save(path, obj):
    with path.open('x') as f:
        json.dump(obj,f,ensure_ascii=False,indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())

def kube(*args):
    cmd = ['kubectl','--kubeconfig=/root/.kube/config','--request-timeout=8s',*args,'-o','json']
    return json.loads(subprocess.check_output(cmd,timeout=10,stderr=subprocess.DEVNULL))

class SQL:
    def __init__(self, command, env, fds):
        from pathlib import Path
        with (Path(__file__).parent/'sql-client.stderr.private.log').open('ab') as error_log:
            self.child = subprocess.Popen(command+['-q'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                stderr=error_log,env=env,pass_fds=fds,bufsize=0)
        self.pending = b''
    def query(self, sql, timeout=24):
        end=time.monotonic()+timeout
        marker='pg18_end_'+uuid.uuid4().hex
        self.child.stdin.write((sql+"\nSELECT '"+marker+"';\n").encode())
        self.child.stdin.flush()
        result=[]
        while time.monotonic()<end:
            while b'\n' in self.pending:
                line,self.pending=self.pending.split(b'\n',1)
                if line.decode()==marker:
                    return result
                if line.startswith(b'{'):
                    result.append(json.loads(line))
            t=end-time.monotonic()
            if t<=0 or not select.select([self.child.stdout],[],[],t)[0]:
                break
            chunk=os.read(self.child.stdout.fileno(),32768)
            if not chunk:
                raise RuntimeError('SQL_CLIENT_EXIT_'+str(self.child.wait(timeout=4)))
            self.pending+=chunk
        raise TimeoutError('SQL_RESPONSE_DEADLINE')
    def close(self):
        if self.child.poll() is None:
            self.child.stdin.close()
        try:
            return self.child.wait(timeout=8)
        except subprocess.TimeoutExpired:
            self.child.terminate()
            return self.child.wait(timeout=5)
    def one(self, sql):
        rows=self.query(sql)
        assert len(rows)==1
        return rows[0]

def ledger(uid):
    vm=kube('-n','neon','get','virtualmachines',VM)
    assert vm['metadata']['uid']==uid and vm['status']['podName']==VM+'-tjhzc'
    encoded=json.loads(vm['metadata']['annotations'][JOURNAL])
    raw=zlib.decompress(base64.b64decode(encoded['data']))
    assert hashlib.sha256(raw).hexdigest()==encoded['sha256']
    return vm,json.loads(raw)

def lsn(value):
    assert re.fullmatch(r'[A-Fa-f0-9]+/[A-Fa-f0-9]+',value)
    high,low=value.split('/')
    return (int(high,16)<<32)+int(low,16)

def quorum(urls,target):
    stop=time.monotonic()+22
    def peer(item):
        name,(url,tenant,timeline)=item
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(url,timeout=3) as r:
            value=json.load(r)
        assert value['tenant_id']==tenant and value['timeline_id']==timeline
        flush,commit=value['flush_lsn'],value['commit_lsn']
        return dict(peer=name,flush_lsn=flush,commit_lsn=commit,
            durable=min(lsn(flush),lsn(commit))>=lsn(target))
    while time.monotonic()<stop:
        votes=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            for key,future in [(k,pool.submit(peer,(k,v))) for k,v in urls.items()]:
                try: votes.append(future.result())
                except (OSError,TimeoutError): votes.append(dict(peer=key,durable=False))
        if len(votes)==3 and all(x['durable'] for x in votes):
            return votes
        time.sleep(1)
    raise RuntimeError('COMMIT_WAL_QUORUM_NOT_REACHED')

def wal_urls(bind):
    tid,tl=bind['tenant_id'],bind['timeline_id']
    assert all(re.fullmatch('[a-f0-9]{32}',j) for j in (tid,tl))
    services=kube('-n','neon','get','svc','safekeeper1','safekeeper2','safekeeper3')
    out={}
    for item in services['items']:
        name=item['metadata']['name']
        ip=item['spec']['clusterIP']
        assert name in ('safekeeper1','safekeeper2','safekeeper3') and ipaddress.ip_address(ip).is_private
        port=next(x['port'] for x in item['spec']['ports'] if x['name']=='http')
        out[name]=(f'http://{ip}:{port}/v1/tenant/{tid}/timeline/{tl}',tid,tl)
    assert len(out)==3
    return out

class Witness:
    def __init__(self, writer, params, bind, urls, out):
        self.writer,self.urls,self.out=writer,urls,out
        self.pids=[]
        self.n=0
        self.params,self.bind=params,bind
        self.base=self.identity(writer)
        self.boot=self.base['boot']
    def identity(self,sql):
        info=sql.one("SELECT json_build_object('role',current_user,'database',current_database(),'tenant',current_setting('neon.tenant_id'),'timeline',current_setting('neon.timeline_id'),'version',current_setting('server_version_num'),'superuser',(SELECT rolsuper FROM pg_roles WHERE rolname=current_user),'pid',pg_backend_pid(),'boot',pg_postmaster_start_time(),'sync',current_setting('synchronous_commit'));")
        assert info['role']=='app_owner' and not info['superuser'] and info['database']==self.params['database'] and info['tenant']==self.bind['tenant_id'] and info['timeline']==self.bind['timeline_id'] and info['version'].startswith('18') and info['sync']=='on'
        return info
    def data(self,sql):
        row=sql.one(f"""SELECT json_build_object('pid',pg_backend_pid(),'boot',pg_postmaster_start_time(),'rows',count(*),'checksum',md5(string_agg(i::text||':'||payload,'|' ORDER BY i)),'marks',(SELECT count(*) FROM {SCHEMA}.marks),'last_mark',(SELECT max(n) FROM {SCHEMA}.marks),'mark_ids',(SELECT md5(string_agg(n::text||':'||phase,',' ORDER BY n)) FROM {SCHEMA}.marks),'persistence',(SELECT relpersistence FROM pg_class WHERE oid='{SCHEMA}.payload'::regclass),'flush',pg_current_wal_flush_lsn(),'after_commit',pg_current_wal_insert_lsn()) FROM {SCHEMA}.payload;""")
        assert row['rows']==512 and row['checksum']==CHECKSUM and row['persistence']=='p' and row['marks']==row['last_mark']==self.n and row['boot']==self.boot
        return row
    def commit(self,phase,create=False):
        assert re.fullmatch('[a-z_]+',phase)
        self.n+=1
        ddl=''
        if create:
            ddl=f"""CREATE SCHEMA {SCHEMA};
CREATE TABLE {SCHEMA}.payload(i integer PRIMARY KEY,payload text NOT NULL);
CREATE TABLE {SCHEMA}.marks(n integer PRIMARY KEY,phase text NOT NULL);
INSERT INTO {SCHEMA}.payload SELECT n,md5('pg18-wal-'||n::text) FROM generate_series(1,512) g(n);"""
        self.writer.query(f"BEGIN; SET LOCAL synchronous_commit=on;{ddl} INSERT INTO {SCHEMA}.marks VALUES({self.n},'{phase}'); COMMIT;")
        row=self.data(self.writer)
        assert row['pid']==self.base['pid']
        votes=quorum(self.urls,row['after_commit'])
        record=dict(at=now(),n=self.n,phase=phase,SQL=row,safekeepers=votes)
        with (self.out/'已提交写入.PUBLIC.jsonl').open('a') as f:
            f.write(json.dumps(record,ensure_ascii=False)+'\n');f.flush();os.fsync(f.fileno())
        return record
    def reconnect(self,command,env,fds):
        other=SQL(command,env,fds)
        try:
            who=self.identity(other)
            data=self.data(other)
            assert who['pid']==data['pid'] and who['pid']!=self.base['pid'] and who['boot']==self.boot
            original=self.data(self.writer)
            assert data['mark_ids']==original['mark_ids']
        finally:
            status=other.close()
        assert status==0
        return dict(data=data,role=who['role'],exit=status)

def pressure(conn,hold):
    if not hold:
        conn.query('ROLLBACK;')
        return
    conn.query("BEGIN READ ONLY; SET LOCAL work_mem='64MB';")
    for num in (1,2):
        cur='dsga_pressure_'+str(num)
        conn.query(f"DECLARE {cur} NO SCROLL CURSOR FOR SELECT n,md5('{num}'||n::text) AS key FROM generate_series(1,400000) g(n) ORDER BY key; FETCH FORWARD 1 FROM {cur};",timeout=70)

def physical(vm,m,history,uid,boot,epoch,prev_gen,prev_limit,kind):
    o=history['operation'];req=o['request'];key=req['key'];op=key['operation_id']
    assert history['outcome']=='succeeded' and key==m['high_water'] and not m['active'] and not m.get('service_closure') and m.get('budget_held')
    assert req['budget_commitment_id']=='automatic-%016x%016x'%(int(op['high']),int(op['low'])) and req['kind']==kind
    assert key['instance']==m['pg_instance'] and key['instance']['vm_uid']==uid and key['instance']['pg_boot_id']==boot and key['issuer_epoch']==epoch==m['owner']['epoch'] and int(req['expected_capacity_generation'])==prev_gen
    steps=o['steps'];assert steps and all(x['phase']=='settled' and x['observation']['result']=='completed' and x['observation']['key']==key for x in steps)
    facts={x['action']:json.loads(base64.b64decode(x['observation']['data'])) for x in steps}
    hard=int(facts['prepare_budget']['limits']['memory_max_bytes']);is_grow=kind=='grow'
    assert hard==(1127743488 if is_grow else 1077411840) and (hard>prev_limit if is_grow else hard<prev_limit)
    ready=facts['guest_ready']['evidence']; assert int(ready['memory_max_bytes'])==hard and int(ready['swap_max_bytes'])==0
    ram=(8 if is_grow else 7)*2**30
    fact=facts['resize_vm']
    assert ram==int(req['target_vm_memory_bytes'])==int(fact['actual_memory_bytes'])==int(fact['qmp_observed_memory_bytes']) and fact['no_pending_commands']
    assert int(vm['status']['memorySize'].removesuffix('Gi'))*2**30==ram
    pg=facts['resume_admission']['response']['observed']
    assert pg['instance']==key['instance'] and pg['admission_open'] and int(pg['capacity_generation'])==prev_gen+1 and int(pg['low_buffers'])==int(pg['high_buffers'])==int(req['target_buffers'])==(2048 if is_grow else 256)
    if not is_grow:
        online=facts['quiesce']; gate=online['dsm']['prepared']['connections']
        assert online['protocol_version']==2 and online['request']==req and int(online['serving_revision'])==int(gate['work']['observed']['admission_revision']) and int(gate['requested_serial'])==int(gate['applied_serial'])>0 and gate['requested_open'] and gate['applied_open'] and gate['acknowledged'] and not gate['public_connections_blocked']
        rel=facts['release_pg']['response']['observed']
        assert rel['operation']['key']==key and {a['array'] for a in rel['arrays']}=={'descriptors','blocks','iocv','checkpoint'} and len(rel['arrays'])==4
        for a in rel['arrays']:
            p=a['provider']
            assert a['release_key']==key and a['release_capacity_generation']==rel['capacity_generation'] and a['release_state']=='verified' and p['status']=='ok' and p['has_result'] and p['physical_release_verified'] and not(a['allocation_unknown'] or p['allocation_may_be_partial'] or p['release_may_be_partial'] or p['system_errno'])
    return dict(key=key,kind=kind,generation=prev_gen+1,PG_buffers=int(pg['high_buffers']),normal_memory_max=hard,QMP_bytes=ram,budget=req['budget_commitment_id'],node=vm['status']['node'],released_arrays=0 if is_grow else 4)

def check_host(action,request,uid):
    node=kube('get','node',action['node'])
    host=json.loads(node['metadata']['annotations'][HOST])
    placement=next(p for p in host['placements'] if p.get('vm_uid')==uid)
    settlement=placement.get('settled') or {}
    if settlement.get('request')==request and int(placement['memory_bytes'])==action['QMP_bytes']:
        return settlement.get('finalization_sha256')
    return None

def execute(command,env,fds,params,binding,out):
    baseline=json.loads((out/'本实例起始实读.PUBLIC.json').read_text())
    uid=params['current_VMUID']; boot=baseline['PG_boot']
    vm,m=ledger(uid);previous=baseline['original_key']
    assert baseline['held_source'] in ('initial_held', 'budget_held')
    held=m.get(baseline['held_source'])
    assert m['high_water']==previous and not m['active'] and not m.get('service_closure') and held and hashlib.sha256(json.dumps(held,sort_keys=True,separators=(',',':')).encode()).hexdigest()==baseline['held_sha256']
    epoch=m['owner']['epoch'];assert epoch==baseline['owner_epoch']
    assert not (out/'write-intent.PUBLIC.json').exists()
    urls=wal_urls(binding)
    writer=SQL(command,env,fds);load=None;success=False;actions=[]
    try:
        witness=Witness(writer,params,binding,urls,out)
        save(out/'write-intent.PUBLIC.json',dict(at=now(),schema=SCHEMA,role=witness.base['role'],writer=witness.base['pid'],vm_uid=uid,pg_boot=boot,original_key=previous))
        witness.commit('baseline',True)
        load=SQL(command,env,fds)
        identity=witness.identity(load)
        assert identity['pid']!=witness.base['pid'] and identity['boot']==witness.boot
        generation=int(baseline['capacity_generation']);limit=int(baseline['normal_memory_max_after'])
        for i,kind in enumerate(ORDER):
            label=('first_grow','normal_shrink','regrow','return_idle')[i]
            start=time.monotonic()
            pre=witness.commit('before_'+label)
            pressure(load,kind=='grow')
            last=0; seen_active=False;seen_writes=0
            while time.monotonic()-start<490:
                vm,m=ledger(uid)
                assert not m.get('service_closure') and m['owner']['epoch']==epoch
                act=m.get('active')
                if act:
                    assert act['request']['kind']==kind and act['request']['key']['instance']['pg_boot_id']==boot
                    seen_active=True
                r=m['history'][-1] if m['history'] else None
                if r and r['operation']['request']['key']!=previous:
                    ar=physical(vm,m,r,uid,boot,epoch,generation,limit,kind)
                    h=check_host(ar,r['operation']['request'],uid)
                    if h:
                        ar['host_finalization_sha256']=h
                        save(out/(label+'-原操作.PUBLIC.json'),r)
                        break
                if time.monotonic()-last>=3:
                    witness.commit('during_'+label)
                    if act:seen_writes+=1
                    last=time.monotonic()
                time.sleep(2)
            else: raise RuntimeError('AUTOMATIC_PHASE_DEADLINE_'+label)
            post=witness.commit('after_'+label)
            read=witness.reconnect(command,env,fds)
            assert seen_active and seen_writes>0
            ar.update(at=now(),label=label,seconds=round(time.monotonic()-start,2),writes_while_in_flight=seen_writes,before_commit=pre['n'],after_commit=post['n'],reconnect=read)
            save(out/(label+'.PUBLIC.json'),ar);print('ACCEPTED',label,ar['key']['issuer_epoch'],ar['key']['revision'],flush=True)
            previous=ar['key'];generation=ar['generation'];limit=ar['normal_memory_max'];actions.append(ar)
        last=witness.commit('complete')
        read=witness.reconnect(command,env,fds)
        success=True
    finally:
        load_exit=load.close() if load else None
        writer_exit=writer.close()
    assert success and load_exit==writer_exit==0
    save(out/'ACCEPTED.PUBLIC.json',dict(at=now(),role='app_owner',writer_pid=witness.base['pid'],postmaster=witness.boot,pg_boot=boot,actions=actions,total_commits=witness.n,last=last,final_reconnect=read,writer_exit=writer_exit,load_exit=load_exit,table=SCHEMA+'.payload'))
