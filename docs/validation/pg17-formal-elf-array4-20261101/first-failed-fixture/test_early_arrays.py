#!/usr/bin/env python3
# 真实Linux PG17早期VA测试；显式test-v1不是Root授权或normal降限证明。
import ctypes,json,os,pwd,re,signal,socket,struct,subprocess,tempfile,time,unittest
from pathlib import Path

BIN=Path(os.environ['NEON_TEST_PG_BIN'])
LIB=Path(os.environ['NEON_TEST_DSG_LIBRARY'])
PROFILE='NEON_DSGA_EARLY_ARRAYS_PROFILE'
AS='NEON_DSGA_ADDRESS_SPACE_MAX_BYTES'
LIMIT=2<<30
USER=pwd.getpwuid(os.geteuid()).pw_name
libc=ctypes.CDLL(None,use_errno=True)
def nnp():
    if libc.prctl(38,1,0,0,0)!=0:
        raise OSError(ctypes.get_errno(),'PR_SET_NO_NEW_PRIVS')
def environment(profile=None,limit=None):
    env={k:v for k,v in os.environ.items() if k not in (PROFILE,AS)}
    if profile is not None: env[PROFILE]=profile
    if limit is not None: env[AS]=str(limit)
    return env

class Peer:
    def __init__(self,path):
        self.s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        try:
            self.s.settimeout(5);self.s.connect(str(path/'.s.PGSQL.55432'))
            data=struct.pack('!I',196608)+f'user\0{USER}\0database\0postgres\0\0'.encode()
            self.s.sendall(struct.pack('!I',len(data)+4)+data)
            messages=self.receive()
            self.pid=next(struct.unpack('!I',v[:4])[0] for k,v in messages if k==b'K')
        except BaseException:
            self.s.close();raise
    def exact(self,n):
        out=b''
        while len(out)<n:
            part=self.s.recv(n-len(out))
            if not part: raise RuntimeError('owned PG connection closed')
            out+=part
        return out
    def receive(self):
        out=[]
        while True:
            kind=self.exact(1);length=struct.unpack('!I',self.exact(4))[0]
            if not 4<=length<=16*1024*1024: raise RuntimeError('invalid PG frame')
            out.append((kind,self.exact(length-4)))
            if kind==b'Z': return out
    def query(self,sql):
        body=sql.encode()+b'\0';self.s.sendall(b'Q'+struct.pack('!I',len(body)+4)+body)
        messages=self.receive()
        if any(k==b'E' for k,_ in messages): raise RuntimeError('owned PG query failed')
        rows=[v for k,v in messages if k==b'D']
        if len(rows)!=1: raise RuntimeError('expected one native fact')
        return json.loads(rows[0][6:])
    def close(self): self.s.close()

class Cluster:
    def __init__(self):
        self.temp=tempfile.TemporaryDirectory(prefix='pg17-birth-',dir='/tmp')
        self.root=Path(self.temp.name);self.data=self.root/'data';self.sock=self.root/'s'
        self.sock.mkdir();self.log=self.root/'postmaster.log';self.process=None;self.peers=[]
        self.run('initdb','-D',self.data,'-A','trust','--no-locale','--no-sync')
        (self.data/'postgresql.conf').write_text(
            f"listen_addresses=''\nport=55432\nunix_socket_directories='{self.sock}'\n"
            f"shared_preload_libraries='{LIB}'\n"
            "enable_dynamic_shared_buffers=on\nshared_buffers='2MB'\nmax_shared_buffers='64MB'\n"
            "huge_pages=off\nmax_connections=20\nlogging_collector=on\n"
            "log_directory='owned-log'\nlog_filename='postgres.log'\n")
    def run(self,binary,*args,check=True):
        r=subprocess.run([str(BIN/binary),*map(str,args)],env=environment(),capture_output=True,text=True,timeout=20)
        if check and r.returncode: raise RuntimeError(r.stderr[-4000:])
        return r
    def sql(self,text,check=True):
        return self.run('psql','-h',self.sock,'-p','55432','-U',USER,'-d','postgres','-XqAt','-v','ON_ERROR_STOP=1','-c',text,check=check)
    def start(self,profile='test-v1:8192',limit=LIMIT,extra=(),check=True):
        self.logstream=self.log.open('a')
        self.process=subprocess.Popen([str(BIN/'postgres'),'-D',str(self.data),*extra],
            env=environment(profile,limit),preexec_fn=nnp,stdout=self.logstream,stderr=subprocess.STDOUT)
        until=time.monotonic()+12
        while time.monotonic()<until:
            if self.process.poll() is not None:
                if check: raise RuntimeError(self.logs()[-7000:])
                return False
            if self.sql('SELECT 1',check=False).returncode==0: return True
            time.sleep(.025)
        raise RuntimeError('owned PG failed to become ready: '+self.logs()[-7000:])
    def logs(self):
        return self.log.read_text()+''.join(f.read_text() for f in (self.data/'owned-log').glob('*'))
    def functions(self):
        self.sql(f"CREATE FUNCTION birth() RETURNS text AS '{LIB}','test_dsg_birth_arrays' LANGUAGE C;"
                 f"CREATE FUNCTION arrays() RETURNS text AS '{LIB}','test_dsg_array_mappings' LANGUAGE C;"
                 f"CREATE FUNCTION grow(bigint,bigint,bigint,int,int,boolean) RETURNS text AS '{LIB}','test_dsg_grow' LANGUAGE C;")
    def peer(self):
        p=Peer(self.sock);self.peers.append(p);return p
    def logger(self,old=None):
        until=time.monotonic()+8
        while time.monotonic()<until:
            for line in subprocess.check_output(['ps','-axo','pid=,ppid=,command='],text=True).splitlines():
                row=line.split(None,2)
                if len(row)==3 and row[1]==str(self.process.pid) and row[2].startswith('postgres: logger') and int(row[0])!=old:
                    return int(row[0])
            time.sleep(.025)
        raise RuntimeError('owned logger absent')
    def close(self):
        for p in self.peers: p.close()
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try: self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill();self.process.wait(timeout=10)
        if hasattr(self,'logstream'): self.logstream.close()
        self.temp.cleanup()

class EarlyArrays(unittest.TestCase):
    def cluster(self):
        pg=Cluster();self.addCleanup(pg.close);return pg
    def invoke(self,profile,limit=LIMIT,nn=True):
        return subprocess.run([str(BIN/'postgres'),'--version'],env=environment(profile,limit),
            preexec_fn=nnp if nn else None,capture_output=True,text=True,timeout=10)
    def test_PG17_U_BIRTH_001_explicit_invalid_modes_and_original_as(self):
        for mode in ('','test-v1:','test-v1:0','test-v1:08192','test-v1:+8192',
                     'test-v1:8192 ','test-v1:2147483648','test-v1:'+'9'*128,
                     'other-v1:8192'):
            with self.subTest(mode=mode):
                r=self.invoke(mode);self.assertEqual(r.returncode,1);self.assertEqual(r.stdout,'')
                self.assertIn('DSGA early arrays:',r.stderr)
        r=self.invoke('root-v1:8192')
        self.assertEqual(r.returncode,1);self.assertEqual(r.stdout,'')
        self.assertIn('DSGA native birth wire:',r.stderr)
        r=self.invoke('test-v1:8192',None)
        self.assertEqual(r.returncode,1);self.assertIn('canonical positive decimal',r.stderr)
        r=self.invoke('test-v1:8192',65536)
        self.assertEqual(r.returncode,1);self.assertIn('strictly smaller',r.stderr)
        r=self.invoke('test-v1:8192',66<<20)
        self.assertEqual(r.returncode,1);self.assertIn('could not reserve complete arrays',r.stderr)
    def test_PG17_U_BIRTH_002_no_nnp_cannot_enable(self):
        self.assertEqual(libc.prctl(39,0,0,0,0),0,'fixture must exercise real NNP=0, no skip')
        r=self.invoke('test-v1:8192',nn=False)
        self.assertEqual(r.returncode,1);self.assertIn('unprivileged no-new-privileges birth',r.stderr)
    def test_PG17_S_BIRTH_001_default_disabled_real_pg(self):
        pg=self.cluster();pg.start(profile=None,limit=None);pg.functions()
        fact=pg.peer().query('SELECT birth()')
        self.assertEqual(fact['phase'],'disabled');self.assertFalse(fact['enabled'])
        self.assertFalse(fact['root_enrolled']);self.assertFalse(fact['source_bound']);self.assertEqual(fact['ranges'],[])
    def test_PG17_S_BIRTH_002_configuration_cannot_drift(self):
        for extra,message in ((('-c','max_shared_buffers=32MB'),'maximum differs'),
                              (('-c','enable_dynamic_shared_buffers=off'),'require dynamic shared buffers')):
            with self.subTest(extra=extra):
                pg=self.cluster();self.assertFalse(pg.start(extra=extra,check=False));self.assertIn(message,pg.logs())
    def test_PG17_S_BIRTH_004_extension_and_native_acl(self):
        pg=self.cluster();pg.start();pg.functions()
        # 必须走真实extension SQL，不能让手工CREATE别名遮掉默认PUBLIC权限。
        pg.sql('CREATE EXTENSION test_dsg_region;CREATE ROLE birth_ordinary LOGIN')
        self.assertEqual(pg.sql("SELECT has_function_privilege('birth_ordinary','test_dsg_birth_arrays()','EXECUTE')").stdout.strip(),'f')
        self.assertEqual(json.loads(pg.sql('SELECT test_dsg_birth_arrays()').stdout)['phase'],'reserved_unbound_test')
        denied=pg.sql('SET ROLE birth_ordinary;SELECT test_dsg_birth_arrays()',check=False)
        self.assertNotEqual(denied.returncode,0);self.assertIn('permission denied for function test_dsg_birth_arrays',denied.stderr)
        # 独立验证C门禁：即使管理员显式GRANT或创建PUBLIC SQL别名仍不能绕过。
        pg.sql('GRANT EXECUTE ON FUNCTION test_dsg_birth_arrays() TO birth_ordinary')
        # definer恢复effective admin但session仍普通，独立覆盖原会话身份门禁。
        pg.sql(f"CREATE FUNCTION birth_definer() RETURNS text AS '{LIB}','test_dsg_birth_arrays' LANGUAGE C SECURITY DEFINER;GRANT EXECUTE ON FUNCTION birth_definer() TO birth_ordinary")
        for query in ('SET ROLE birth_ordinary;SELECT test_dsg_birth_arrays()',
                      'SET SESSION AUTHORIZATION birth_ordinary;SELECT birth()',
                      'SET SESSION AUTHORIZATION birth_ordinary;SELECT birth_definer()'):
            denied=pg.sql(query,check=False)
            self.assertNotEqual(denied.returncode,0)
            self.assertIn('birth array observation requires original administrator',denied.stderr)
    def maps(self,pid,label):
        raw=Path(f'/proc/{pid}/maps').read_text();rows=[]
        target=os.environ.get('NEON_BIRTH_EVIDENCE')
        if target:
            path=Path(target)/self._testMethodName;path.mkdir(exist_ok=True)
            (path/f'{label}-{pid}.maps').write_text(raw)
        for line in raw.splitlines():
            fields=line.split();a,b=fields[0].split('-');rows.append((int(a,16),int(b,16),fields[1]))
        return rows
    def covered(self,rows,left,right,perm):
        cursor=left
        for a,b,p in rows:
            if p==perm and a<=cursor<b:
                cursor=min(right,b)
                if cursor==right:return True
        return False
    def verify(self,pg,peer,logger,label):
        fact=peer.query('SELECT birth()');actual=peer.query('SELECT arrays()')
        self.assertTrue(fact['enabled']);self.assertTrue(fact['imported'])
        self.assertEqual(fact['phase'],'reserved_unbound_test')
        self.assertFalse(fact['root_enrolled']);self.assertFalse(fact['source_bound'])
        self.assertEqual(fact['maximum_buffers'],8192);self.assertEqual(int(fact['address_space_max_bytes']),LIMIT)
        self.assertEqual(len(fact['ranges']),4);page=int(fact['page_size_bytes'])
        total=0;spans=[]
        for r,a in zip(fact['ranges'],actual['arrays']):
            start,size=int(r['base_address']),int(r['reserved_bytes']);total+=size
            self.assertEqual(size,((8192*int(r['element_bytes'])+page-1)//page)*page)
            self.assertEqual(start%page,0);self.assertEqual(start,int(a['address']));self.assertEqual(size,int(a['mapped_bytes']))
            spans.append((start,start+size))
        self.assertEqual(total,int(fact['reserved_total_bytes']));self.assertLess(total,LIMIT)
        for left,right in zip(sorted(spans),sorted(spans)[1:]):self.assertLessEqual(left[1],right[0])
        for pid,perm,role in ((pg.process.pid,'rw-s','pm'),(peer.pid,'rw-s','backend'),(logger,'---p','logger')):
            rows=self.maps(pid,label+'-'+role)
            for a,b in spans:self.assertTrue(self.covered(rows,a,b,perm),(role,a,b,perm))
        return fact,actual
    def test_PG17_S_BIRTH_003_real_logger_and_recovery_keep_r(self):
        pg=self.cluster();pg.start();pg.functions();peer=pg.peer();logger=pg.logger()
        pg.sql("CREATE TABLE probe AS SELECT i,md5(i::text) v FROM generate_series(1,100) i;CHECKPOINT")
        expected=pg.sql("SELECT count(*),sum(i),md5(string_agg(v,',' ORDER BY i)) FROM probe").stdout
        original,old=self.verify(pg,peer,logger,'initial')
        self.assertIn('committed high=512',pg.sql('SELECT grow(1,711,1,512,2000,false)').stdout)
        os.kill(logger,signal.SIGKILL);logger=pg.logger(old=logger)
        self.assertEqual(self.verify(pg,peer,logger,'logger-restarted')[0],original)
        for cycle in range(3):
            os.kill(peer.pid,signal.SIGKILL);until=time.monotonic()+15;new=None
            while time.monotonic()<until:
                try:
                    peer=pg.peer();new=peer.query('SELECT arrays()')
                    if new['boot_id']!=old['boot_id']:break
                except (OSError,RuntimeError,StopIteration):pass
                time.sleep(.025)
            self.assertIsNotNone(new);self.assertNotEqual(new['boot_id'],old['boot_id'])
            fact,new=self.verify(pg,peer,logger,'recovery-'+str(cycle))
            self.assertEqual(fact,original);self.assertEqual(new['arrays'],old['arrays'])
            self.assertEqual(pg.sql("SELECT count(*),sum(i),md5(string_agg(v,',' ORDER BY i)) FROM probe").stdout,expected)
            old=new

if __name__=='__main__':unittest.main(verbosity=2)
