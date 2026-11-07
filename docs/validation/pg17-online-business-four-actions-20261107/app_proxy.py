from pathlib import Path
import argparse,base64,hashlib,hmac,json,os,re,subprocess,sys
SQL_EXECUTION_ENABLED=True
if not SQL_EXECUTION_ENABLED:raise SystemExit('STATIC_PREPARED_ONLY_NO_SQL_OR_CREDENTIAL_READ')
p=Path(__file__).resolve().parent
a=argparse.ArgumentParser();a.add_argument('--parameters',required=True);a.add_argument('--publisher-receipt',required=True);a.add_argument('--mode',choices=['app-wal'],required=True);args=a.parse_args()
v=json.loads(Path(args.parameters).read_text())
if v.get('execution_allowed')is not True or v.get('fresh_new_UID_input_required')is not False:raise SystemExit('ROOT_FRESH_BIND_AND_ENABLE_REQUIRED')
ip=Path(v['route_input_path'])
if hashlib.sha256(ip.read_bytes()).hexdigest()!=v['route_input_sha256']:raise SystemExit('ROUTE_INPUT_HASH_DIFFERS')
source=json.loads(ip.read_text());b=source['binding']
if b['vm_uid']=='e910b360-4ba4-436f-b4d1-f8a3d407d439':raise SystemExit('E910_CURRENT_ROUND_NEVER_PUBLISH_OR_SQL')
receipt=json.loads(Path(args.publisher_receipt).read_text())
for key in ['vm_uid','incarnation','config_secret_uid','config_sha256','secret_data_sha256']:
 if receipt.get(key)!=b[key]:raise SystemExit('FORMAL_PUBLISHER_RECEIPT_SOURCE_DIFFERS')
if receipt.get('selector')!=v['proxy_selector']or receipt.get('registry_uid')!=source['registry_uid']or not re.fullmatch('[0-9a-f]{64}',receipt.get('native_initialization_sha256','')):raise SystemExit('FRESH_FORMAL_NATIVE_AND_PUBLISHED_ROUTE_REQUIRED')
env=os.environ.copy();env['KUBECONFIG']='/root/.kube/config'
def get(kind,name):
 q=subprocess.run(['kubectl','get',kind,name,'-n',b['namespace'],'-o','json'],env=env,capture_output=True,timeout=10)
 if q.returncode:raise RuntimeError('READONLY_KUBE_SOURCE_UNAVAILABLE')
 return json.loads(q.stdout)
def datahash(data):return hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':')).encode()).hexdigest()
vm=get('virtualmachines.vm.neon.tech','cp-d8d85ed796068ab5');c=get('secret','cp-d8d85ed796068ab5-config')
if vm['metadata']['uid']!=b['vm_uid']or vm['spec']['dsgaBootstrap']['incarnation']!=b['incarnation']or c['metadata']['uid']!=b['config_secret_uid']or datahash(c['data'])!=b['secret_data_sha256']:raise SystemExit('FRESH_VM_OR_CONFIG_SOURCE_DIFFERS')
if hashlib.sha256(base64.b64decode(c['data']['config.json'])).hexdigest()!=b['config_sha256']:raise SystemExit('FRESH_CONFIG_BYTES_DIFFER')
credentials=get('secret','cp-d8d85ed796068ab5-credentials')
if credentials['metadata']['uid']!=b['credential_secret_uid']or credentials.get('immutable')is not True or len(credentials['data'])!=4 or datahash(credentials['data'])!=b['credential_data_sha256']:raise SystemExit('ORIGINAL_CREDENTIAL_SOURCE_DIFFERS')
from pathlib import Path
import stat
# 原创建回执绑定应用来源，仅读本实例 app_owner 一项并校验当前正式 verifier。
creds_file=Path('/data/neon-k8s-pg17/private/compute-template-20261103T050813Z-fc8432/business-credentials-v2.private.json')
receipt_file=Path('/data/neon-k8s-pg17/runs/broker-credentials-intent-20261103T053142Z-0287cb/public-field-contract-and-intent-receipt.json')
st=creds_file.lstat()
if not stat.S_ISREG(st.st_mode) or st.st_uid!=0 or st.st_mode & 0o077: raise RuntimeError('APPLICATION_SOURCE_PERMISSIONS')
raw=creds_file.read_bytes()
if hashlib.sha256(raw).hexdigest()!=json.loads(receipt_file.read_text())['business_source_sha256']: raise RuntimeError('APPLICATION_SOURCE_PROVENANCE')
app=json.loads(raw)['app_owner'];del raw
password=app['password'];verifier=base64.b64decode(credentials['data']['applicationVerifier'],validate=True).decode('ascii')
if not hmac.compare_digest(app['scram_verifier'],verifier): raise RuntimeError('APPLICATION_CURRENT_VERIFIER_DIFFERS')
algorithm,params,proof=verifier.split('$');iters,salt=params.split(':');stored,server=proof.split(':')
if algorithm!='SCRAM-SHA-256' or not (4096<=int(iters)<=1000000): raise RuntimeError('APPLICATION_SCRAM_SHAPE')
salted=hashlib.pbkdf2_hmac('sha256',password.encode(),base64.b64decode(salt,validate=True),int(iters))
if not (hmac.compare_digest(hashlib.sha256(hmac.new(salted,b'Client Key',hashlib.sha256).digest()).digest(),base64.b64decode(stored,validate=True)) and hmac.compare_digest(hmac.new(salted,b'Server Key',hashlib.sha256).digest(),base64.b64decode(server,validate=True))): raise RuntimeError('APPLICATION_ORIGINAL_SCRAM_REJECTED')
del app,verifier,salted
proxy=get('service','proxy')
if proxy['metadata']['uid']!=v['expected_proxy_ServiceUID']or not any(x['port']==v['proxy_port']for x in proxy['spec']['ports']):raise SystemExit('PROXY_SERVICE_SOURCE_DIFFERS')
q=subprocess.run(['kubectl','get','secret',v['ca_secret_name'],'-n',b['namespace'],'-o',r'jsonpath={.data.ca\.crt}'],env=env,capture_output=True,timeout=10)
if q.returncode:raise SystemExit('PUBLIC_CA_UNAVAILABLE')
ca=base64.b64decode(q.stdout)
if hashlib.sha256(ca).hexdigest()!=v['ca_sha256']:raise SystemExit('PUBLIC_CA_SOURCE_DIFFERS_REFRESH_BINDING')
q=subprocess.run(['openssl','x509','-noout','-ext','basicConstraints'],input=ca,capture_output=True,timeout=5)
if q.returncode or b'CA:TRUE'not in q.stdout:raise SystemExit('CA_CERT_REQUIRED_NOT_LEAF')
root=Path(v['client_root']);client=root/'usr/local/bin/psql'
if hashlib.sha256(client.read_bytes()).hexdigest()!=v['psql_sha256']:raise SystemExit('FROZEN_PSQL_CLIENT_DIFFERS')
fds=[]
def memfile(label,data):
 fd=os.memfd_create(label,0);os.fchmod(fd,0o600);os.write(fd,data);os.lseek(fd,0,0);fds.append(fd);return '/proc/self/fd/'+str(fd)
try:
 capath=memfile('pg17-proxy-public-ca',ca)
 role=v['data_role'];pw=password.replace('\\','\\\\').replace(':','\\:')
 passpath=memfile('pg17-proxy-probe-pass',(':'.join([v['proxy_host'],str(v['proxy_port']),v['database'],role,pw])+'\n').encode())
 service='[pg17probe]\nhost='+v['proxy_host']+'\nhostaddr='+proxy['spec']['clusterIP']+'\nport='+str(v['proxy_port'])+'\ndbname='+v['database']+'\nuser='+role+'\nsslmode=verify-full\nsslrootcert='+capath+'\noptions=endpoint='+v['proxy_selector']+'\nconnect_timeout=10\n'
 servicepath=memfile('pg17-proxy-service',service.encode())
 sql="SELECT json_build_object('version',version(),'server_version_num',current_setting('server_version_num'),'role',current_user,'database',current_database(),'tenant_id',current_setting('neon.tenant_id',true),'timeline_id',current_setting('neon.timeline_id',true),'postmaster_start',pg_postmaster_start_time()::text,'backend_pid',pg_backend_pid(),'admission_lease_timeout',current_setting('dynamic_shared_buffers_admission_lease_timeout',true),'require_admission',current_setting('dynamic_shared_buffers_require_admission',true),'session_user',session_user,'can_connect',has_database_privilege(current_user,current_database(),'CONNECT'),'can_temp',has_database_privilege(current_user,current_database(),'TEMPORARY'),'default_transaction_read_only',current_setting('default_transaction_read_only'),'transaction_read_only',current_setting('transaction_read_only'),'in_recovery',pg_is_in_recovery());"
 libs=[str(root/x)for x in ['lib64','lib/x86_64-linux-gnu','usr/lib/x86_64-linux-gnu','usr/local/lib','lib','usr/lib']if(root/x).exists()]
 command=[str(root/'lib64/ld-linux-x86-64.so.2'),'--library-path',':'.join(libs),str(client),'-X','-w','-A','-t','-v','ON_ERROR_STOP=1','-c',sql]
 childenv={'PATH':'/usr/bin:/bin','LC_ALL':'C','PGSERVICE':'pg17probe','PGSERVICEFILE':servicepath,'PGPASSFILE':passpath,'PGAPPNAME':v['nonce']}
 if command[-2:]!=['-c',sql]:raise RuntimeError('CLIENT_ARGUMENTS_CHANGED')
 from durable_case import execute
 execute(command[:-2],childenv,tuple(fds),v,b,p)
finally:
 for fd in fds:os.close(fd)
