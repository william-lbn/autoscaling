#!/usr/bin/env python3
# TEST_ONLY: 真PG+真map_files/预触页；Root策略/预算/JWT/normal资格不在此fixture范围。
import ctypes, hashlib, json, mmap, os, pwd, signal, socket, stat, struct, subprocess, tempfile, threading, time, unittest
from pathlib import Path
import test_early_arrays as early
BIN=early.BIN
LIB=early.LIB
ACCOUNT=pwd.getpwnam('nonroot')
early.USER=ACCOUNT.pw_name
SO_PEERPIDFD=77
ROOT_PATH=Path('/run/neon-dsga')
SOCKET_PATH=ROOT_PATH/'array-populate.sock'
libc=ctypes.CDLL(None,use_errno=True)
def child_identity():
    os.setgroups([]);os.setgid(ACCOUNT.pw_gid);os.setuid(ACCOUNT.pw_uid);early.nnp()
def rounded(n):return (n+4095)//4096*4096
def u64(raw,n):return struct.unpack_from('!Q',raw,n)[0]

def send_pidfd(pid,sig):
    fd=os.pidfd_open(pid,0)
    try:signal.pidfd_send_signal(fd,sig)
    finally:os.close(fd)

class RootFixture:
    """仅测试Root responder；读取真实peer/四inode，不产生产Root授权来源。"""
    def __init__(self, fault=None):
        ROOT_PATH.mkdir(mode=0o755,exist_ok=True);os.chmod(ROOT_PATH,0o755)
        if SOCKET_PATH.exists():raise RuntimeError('fresh container fixture socket already exists')
        self.s=socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET|socket.SOCK_CLOEXEC)
        self.s.bind(str(SOCKET_PATH));os.chmod(SOCKET_PATH,0o666);self.s.listen(8);self.s.settimeout(.1)
        self.stop=threading.Event();self.events=[];self.failures=[];self.generation=None;self.fds=[];self.maps=[]
        self.regnonce=bytes.fromhex('44'*32);self.fault=fault;self.fault_used=False
        self.worker=threading.Thread(target=self.run);self.worker.start()
    def run(self):
        while not self.stop.is_set():
            try:conn,_=self.s.accept()
            except TimeoutError:continue
            except OSError:
                if self.stop.is_set():return
                raise
            try:
                with conn:self.handle(conn)
            except BaseException as e:self.failures.append(repr(e))
    def handle(self,conn):
        raw=conn.recv(513)
        if len(raw)!=512 or raw[:8]!=b'NDSGARR1':raise RuntimeError('wrong exact array request')
        if struct.unpack_from('!I',raw,8)[0]!=1 or any(raw[480:]):raise RuntimeError('noncanonical request')
        pid,uid,gid=struct.unpack('3i',conn.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
        if uid!=ACCOUNT.pw_uid:raise RuntimeError('unexpected PG peer uid')
        # 内核原socket peer pidfd，不以readlink文本或客户端传FD代替。
        peerfd=conn.getsockopt(socket.SOL_SOCKET,SO_PEERPIDFD)
        try:
            os.set_inheritable(peerfd,False)
            if raw[112:144]!=b'\x11'*32 or raw[144:176]!=b'\x22'*32 or raw[176:208]!=b'\x33'*32:raise RuntimeError('test profile source differs')
            op=struct.unpack_from('!I',raw,12)[0];gen=u64(raw,104);boot=u64(raw,16)
            ranges=[tuple(u64(raw,272+i*40+j*8) for j in range(5)) for i in range(4)]
            event={'op':op,'pid':pid,'uid':uid,'boot':str(boot),'mapping_generation':gen,'array':u64(raw,80),'offset':u64(raw,88),'bytes':u64(raw,96),'high':u64(raw,472),'target':u64(raw,464)}
            if op==1:
                if self.generation is not None and (gen<=self.generation or not any(e['op']==3 and e['mapping_generation']==self.generation for e in self.events)):raise RuntimeError('new mapping without old retire begin')
                opened=[];maps=[];identities=[]
                try:
                    rows=Path(f'/proc/{pid}/maps').read_text().splitlines()
                    for base,logical,span,element,page in ranges:
                        if page!=4096 or logical!=u64(raw,32)*element or span!=rounded(logical):raise RuntimeError('wrong compiled geometry')
                        name=f'{base:x}-{base+span:x}'
                        row=next(x.split() for x in rows if x.split()[0]==name)
                        if row[1]!='rw-s' or int(row[2],16)!=0 or int(row[4])==0:raise RuntimeError('not exact shared anonymous shmem VMA')
                        fd=os.open(f'/proc/{pid}/map_files/{name}',os.O_RDWR|os.O_CLOEXEC)
                        st=os.fstat(fd)
                        if not stat.S_ISREG(st.st_mode) or st.st_ino!=int(row[4]) or st.st_size!=span:raise RuntimeError('actual backing inode/span differs')
                        dev=[int(v,16) for v in row[3].split(':')]
                        if (os.major(st.st_dev),os.minor(st.st_dev))!=tuple(dev):raise RuntimeError('actual backing dev differs')
                        fs=ctypes.create_string_buffer(256)
                        if libc.fstatfs(fd,fs)!=0 or ctypes.c_long.from_buffer(fs).value!=0x01021994:raise RuntimeError('not kernel shmem backing')
                        opened.append(fd);maps.append(mmap.mmap(fd,span,flags=mmap.MAP_SHARED,prot=mmap.PROT_READ|mmap.PROT_WRITE));identities.append([st.st_dev,st.st_ino,st.st_size])
                    if self.fds:
                        old={(os.fstat(f).st_dev,os.fstat(f).st_ino) for f in self.fds}
                        if old & {tuple(x[:2]) for x in identities}:raise RuntimeError('Rearm reused old backing inode')
                    # 仅见完整新四inode才退旧Root FD；retire_begin本身不清旧账。
                    for m in self.maps:m.close()
                    for f in self.fds:os.close(f)
                    self.fds,self.maps,self.ranges=opened,maps,ranges;opened=[];maps=[];self.generation=gen;self.boot=boot
                    event['actual_backing']=identities
                finally:
                    for m in maps:m.close()
                    for f in opened:os.close(f)
            else:
                if gen!=self.generation or boot!=self.boot or raw[432:464]!=self.regnonce or ranges!=self.ranges:raise RuntimeError('old generation/FD ticket changed')
                # backend也须精确映射同四inode；普通peer不能替换任意shmem。
                for (base,logical,span,element,page),fd in zip(ranges,self.fds):
                    f=os.open(f'/proc/{pid}/map_files/{base:x}-{base+span:x}',os.O_RDWR|os.O_CLOEXEC)
                    try:
                        a,b=os.fstat(f),os.fstat(fd)
                        if (a.st_dev,a.st_ino,a.st_size)!=(b.st_dev,b.st_ino,b.st_size):raise RuntimeError('request peer has different backing')
                    finally:os.close(f)
                if op==2:
                    i,off,n=u64(raw,80),u64(raw,88),u64(raw,96)
                    if i>=4 or off%4096 or n==0 or n%4096 or off+n>ranges[i][2]:raise RuntimeError('populate outside original geometry')
                    # 真VMA MADV_POPULATE_WRITE，不写业务字节，不用RSS判断预触结果。
                    address=ctypes.addressof(ctypes.c_char.from_buffer(self.maps[i]))+off
                    if libc.madvise(ctypes.c_void_p(address),ctypes.c_size_t(n),23)!=0:raise OSError(ctypes.get_errno(),'Root fixture MADV_POPULATE_WRITE')
                    event['actual_populated']=True
                elif op!=3:raise RuntimeError('unknown operation')
            self.events.append(event)
            if self.fault and not self.fault_used and (self.fault=='startup_drop' and op==2 and u64(raw,464)==0 or self.fault=='grow_drop' and op==2 and u64(raw,464)>0):
                self.fault_used=True;event['dropped_reply']=True;return
            reply=bytearray(160);reply[:8]=b'NDSGARA1';struct.pack_into('!IIIIQQ',reply,8,1,0,0,0,u64(raw,88),u64(raw,96));reply[40:72]=hashlib.sha256(raw).digest();reply[72:104]=self.regnonce;struct.pack_into('!QQ',reply,104,1,gen)
            if self.fault=='bad_digest' and not self.fault_used:self.fault_used=True;reply[40]^=1
            if conn.send(reply)!=160:raise RuntimeError('short fixture reply')
        finally:os.close(peerfd)
    def assert_clean(self):
        if self.failures:raise AssertionError(self.failures)
    def close(self):
        self.stop.set();self.s.close();self.worker.join(timeout=5)
        for m in self.maps:m.close()
        for f in self.fds:os.close(f)
        SOCKET_PATH.unlink(missing_ok=True)

class Cluster(early.Cluster):
    def __init__(self):
        # initdb/postgres全为真实非特权uid；Root只运行隔离测试responder。
        self.temp=tempfile.TemporaryDirectory(prefix='pg17-array-root-',dir='/tmp')
        self.root=Path(self.temp.name);os.chown(self.root,ACCOUNT.pw_uid,ACCOUNT.pw_gid)
        self.data=self.root/'data';self.sock=self.root/'s';self.sock.mkdir();os.chown(self.sock,ACCOUNT.pw_uid,ACCOUNT.pw_gid)
        self.log=self.root/'postmaster.log';self.process=None;self.peers=[]
        self.run('initdb','-D',self.data,'-A','trust','--no-locale','--no-sync')
        (self.data/'postgresql.conf').write_text(f"listen_addresses=''\nport=55432\nunix_socket_directories='{self.sock}'\nshared_preload_libraries='{LIB}'\nenable_dynamic_shared_buffers=on\nshared_buffers='2MB'\nmax_shared_buffers='64MB'\nhuge_pages=off\nmax_connections=20\nlogging_collector=on\nlog_directory='owned-log'\nlog_filename='postgres.log'\n")
        os.chown(self.data/'postgresql.conf',ACCOUNT.pw_uid,ACCOUNT.pw_gid)
    def run(self,binary,*args,check=True):
        r=subprocess.run([str(BIN/binary),*map(str,args)],env=early.environment(),preexec_fn=child_identity,capture_output=True,text=True,timeout=20)
        if check and r.returncode:raise RuntimeError(r.stderr[-5000:])
        return r
    def start(self,check=True):
        self.logstream=self.log.open('a')
        env=early.environment('test-v1:8192',2<<30);env['NEON_DSGA_ARRAY_ROOT_PROFILE']='test-v1'
        self.process=subprocess.Popen([str(BIN/'postgres'),'-D',str(self.data)],env=env,preexec_fn=child_identity,stdout=self.logstream,stderr=subprocess.STDOUT)
        until=time.monotonic()+15
        while time.monotonic()<until:
            if self.process.poll() is not None:
                if check:raise RuntimeError(f'actual PG exit={self.process.returncode}; '+self.logs()[-9000:])
                return False
            if self.sql('SELECT 1',check=False).returncode==0:return True
            time.sleep(.025)
        raise RuntimeError('PG startup deadline: '+self.logs()[-9000:])
    def functions(self):
        self.sql(f"CREATE FUNCTION ctl(text,text,text,text,text,text,text,int,int,int,int) RETURNS jsonb AS '{LIB}','test_dsg_control' LANGUAGE C STRICT;CREATE FUNCTION arrays() RETURNS text AS '{LIB}','test_dsg_array_mappings' LANGUAGE C;")
    def state(self):return json.loads(self.sql("SELECT ctl('state','0','0','0','0','0','0',0,0,0,0)").stdout)['observed']
    def resize(self,target,revision,generation,kind=0):
        boot=self.state()['boot']
        return json.loads(self.sql(f"SELECT ctl('resize','{boot}','1','{revision}','0','{revision}','{generation}',{target},{kind},5000,1)").stdout)

class ArrayRootNative(unittest.TestCase):
    def fixture(self,fault=None):
        root=RootFixture(fault);self.addCleanup(root.close)
        pg=Cluster();self.addCleanup(pg.close);return root,pg
    def test_PG17_S_ARRAY_ROOT_001_initial_grow_shrink_regrow_real_backing(self):
        root,pg=self.fixture();pg.start();pg.functions();root.assert_clean()
        self.assertEqual([e['op'] for e in root.events],[1,2,2,2,2]);self.assertEqual(len(root.events[0]['actual_backing']),4)
        pg.sql("CREATE TABLE payload AS SELECT i,md5(i::text) v FROM generate_series(1,500) i;CHECKPOINT")
        checksum=pg.sql("SELECT count(*),sum(i),md5(string_agg(v,',' ORDER BY i)) FROM payload").stdout
        for target,rev,gen,kind in [(1024,1,1,0),(256,2,2,1),(768,3,3,0)]:
            fact=pg.resize(target,rev,gen,kind);self.assertIsNotNone(fact['result'],(target,rev,gen,kind,fact));self.assertEqual(fact['result']['phase'],2,fact);self.assertEqual(fact['observed']['high'],target)
        self.assertEqual(pg.sql("SELECT count(*),sum(i),md5(string_agg(v,',' ORDER BY i)) FROM payload").stdout,checksum)
        root.assert_clean();self.assertTrue(any(e['high']==256 and e['target']==768 for e in root.events))
    def test_PG17_S_ARRAY_ROOT_002_bad_or_lost_startup_reply_fails_closed(self):
        for fault in ('bad_digest','startup_drop'):
            with self.subTest(fault=fault):
                root=RootFixture(fault);pg=Cluster()
                try:
                    self.assertFalse(pg.start(check=False));self.assertNotEqual(pg.process.returncode,0);self.assertIn('Root',pg.logs());self.assertTrue(root.fault_used);root.assert_clean()
                finally:pg.close();root.close()
    def test_PG17_S_ARRAY_ROOT_003_lost_grow_reply_retains_unknown_cost(self):
        root,pg=self.fixture('grow_drop');pg.start();pg.functions();before=pg.state();fact=pg.resize(1024,1,1)
        self.assertEqual(fact['result']['phase'],3);self.assertEqual(fact['observed']['high'],before['high']);self.assertEqual(fact['observed']['generation'],before['generation'])
        self.assertTrue(any(a['unknown'] for a in fact['observed']['arrays']),fact)
        self.assertTrue(root.fault_used);self.assertTrue(any(e.get('actual_populated') and e.get('dropped_reply') for e in root.events));root.assert_clean()
    def test_PG17_S_ARRAY_ROOT_004_logger_and_reinit_keep_old_backing_until_new_registration(self):
        root,pg=self.fixture();pg.start();pg.functions();peer=pg.peer();logger=pg.logger();old=pg.state()['boot']
        pg.sql('CREATE TABLE recovery_data AS SELECT i FROM generate_series(1,100) i;CHECKPOINT')
        send_pidfd(logger,signal.SIGKILL);pg.logger(old=logger);self.assertFalse(any(e['op']==3 for e in root.events))
        for cycle in range(3):
            send_pidfd(peer.pid,signal.SIGKILL);until=time.monotonic()+15;new=None
            while time.monotonic()<until:
                try:
                    peer=pg.peer();new=pg.state()['boot']
                    if new!=old:break
                except (OSError,RuntimeError,StopIteration):pass
                time.sleep(.025)
            self.assertIsNotNone(new,pg.logs()[-8000:]);self.assertNotEqual(new,old);old=new
            self.assertEqual(pg.sql('SELECT count(*),sum(i) FROM recovery_data').stdout.strip(),'100|5050')
        self.assertEqual(len([e for e in root.events if e['op']==1]),4);self.assertGreaterEqual(len([e for e in root.events if e['op']==3]),3);root.assert_clean()
    def tearDown(self):
        target=os.environ.get('NEON_ARRAY_ROOT_EVIDENCE')
        if target:
            p=Path(target)/self._testMethodName;p.mkdir(parents=True,exist_ok=True)
            for f in self._cleanups:
                obj=getattr(f[0],'__self__',None)
                if isinstance(obj,RootFixture):(p/'root-events.json').write_text(json.dumps({'events':obj.events,'failures':obj.failures},indent=2))
                if isinstance(obj,Cluster):(p/'postgres.log').write_text(obj.logs())

if __name__=='__main__':unittest.main(verbosity=2)
