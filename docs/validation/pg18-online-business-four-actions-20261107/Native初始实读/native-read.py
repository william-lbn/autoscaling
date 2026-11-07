import subprocess,json,base64,hashlib,datetime,time,urllib.request,pathlib,os
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization
r=pathlib.Path(__file__).parent;k=['kubectl','--kubeconfig=/root/.kube/config'];name='cp-24c41e23bb104ead';uid='02679a05-092f-4a50-b4f2-391fae7ac63e';inc='ba001d5a36fb46ea99b80b8aac1a9ee9'
def get(kind,n):return json.loads(subprocess.check_output(k+['-n','neon','get',kind,n,'-o','json']))
def stable():
 v=get('virtualmachines.vm.neon.tech',name);assert v['metadata']['uid']==uid and v['spec']['dsgaBootstrap']['incarnation']==inc and v['spec']['dsgaBootstrap']['ready'] and v['status']['phase']=='Running'
 s=get('secrets',name+'-config');assert s['metadata']['uid']=='fa9ff8c4-a20f-494a-9018-84cad1d35c66' and s['immutable'] and v['spec']['dsgaBootstrap']['configSecretUID']==s['metadata']['uid']
 m=get('services',name+'-management');assert m['metadata']['uid']=='85787e76-9cde-48ac-a453-d11c29c6bfb2' and m['spec']['type']=='ClusterIP' and m['spec']['selector']=={'vm.neon.tech/name':name} and m['spec']['ports'][0]['port']==3080 and any(o['uid']==uid for o in m['metadata'].get('ownerReferences',[]))
 c=get('secrets',name+'-credentials');assert c['metadata']['uid']=='a7f31b74-1deb-417e-8113-87fe2e50e331' and c['immutable'] and len(c['data'])==4
 return v,s,m,c
def original_journal(v):
 import zlib
 w=json.loads(v['metadata']['annotations']['internal.autoscaling.neon.tech/dsga-journal-v1']);assert w['version']==1 and w['encoding']=='zlib-v1' and w['incarnation']==inc
 z=zlib.decompressobj();raw=z.decompress(base64.b64decode(w['data']),1048577);assert len(raw)<=1048576 and z.eof and not z.unused_data and not z.unconsumed_tail and hashlib.sha256(raw).hexdigest()==w['sha256'];return json.loads(raw)
out={'scope':'original authenticated native capabilities/memory GET only','VMUID':uid,'incarnation':inc,'post_requests':0,'published':False,'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'requests':[]}
try:
 v,s,m,c=stable();j=original_journal(v);epoch=j['owner']['epoch'];assert int(epoch)>0;out['observed_journal_epoch_before']=epoch
 seed=base64.b64decode(c['data']['controlSigningSeed']);assert len(seed)==32;key=Ed25519PrivateKey.from_private_bytes(seed);pub=key.public_key().public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw);x=base64.urlsafe_b64encode(pub).decode().rstrip('=');cfg=json.loads(base64.b64decode(s['data']['config.json']))
 def jwks(o):
  if isinstance(o,dict):
   if o.get('kty')=='OKP' and o.get('crv')=='Ed25519' and o.get('x')==x:yield o
   for z in o.values():yield from jwks(z)
  elif isinstance(o,list):
   for z in o:yield from jwks(z)
 kid=hashlib.sha256(pub).digest()[:16].hex();assert any(q.get('kid')==kid for q in jwks(cfg))
 def enc(o):return base64.urlsafe_b64encode(json.dumps(o,separators=(',',':')).encode()).decode().rstrip('=')
 class NoRedirect(urllib.request.HTTPRedirectHandler):
  def redirect_request(self,*args,**kw):return None
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
 for endpoint in ['capabilities','memory','initialization']:
  now=int(time.time());claims={'aud':'neon-dsga','scope':('compute_ctl:dsga_initialization' if endpoint=='initialization' else 'compute_ctl:dsga'),'compute_id':name,'vm_uid':uid,'compute_instance_id':name,'issuer_epoch':('1' if endpoint=='initialization' else str(epoch)),'nbf':now-1,'exp':now+60};inp=enc({'alg':'EdDSA','typ':'JWT','kid':kid})+'.'+enc(claims);token=inp+'.'+base64.urlsafe_b64encode(key.sign(inp.encode())).decode().rstrip('=');req=urllib.request.Request('http://'+m['spec']['clusterIP']+':3080/dsg/v1/'+endpoint,headers={'Authorization':'Bearer '+token,'Cache-Control':'no-store','Accept':'application/json','Accept-Encoding':'identity'},method='GET');item={'endpoint':endpoint,'method':'GET','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
  try:
   with opener.open(req,timeout=25) as h:raw=h.read(1048577);status=h.status;headers=dict(h.headers)
  except urllib.error.HTTPError as e:raw=e.read(1048577);status=e.code;headers=dict(e.headers)
  assert len(raw)<=1048576;item.update(http_status=status,response_sha256=hashlib.sha256(raw).hexdigest(),response_bytes=len(raw));p=r/(endpoint+'-response.private.json');p.write_bytes(raw);os.chmod(p,0o600);f=json.loads(raw)
  if status==200:
   item['protocol_version']=f.get('protocol_version');item['vm_shrink_permit']=f.get('vm_shrink_permit');item['keys']=sorted(f.keys());o=f.get('observed',f)
   item['typed_fact']={k:o[k] for k in ['instance','issuer_floor_epoch','capacity_generation','admission_open','admission_revision','low_buffers','maximum_buffers','n_buffers','actual_buffers','buffer_generation'] if k in o}
  else:
   item['fixed_code']=f.get('state') if f.get('state') in ['initialization_unconfirmed','not_available','observation_unavailable','management_unavailable','wrong_instance','invalid_credential','invalid_context','execution_failed'] else None
  out['requests'].append(item)
 v2,s2,m2,c2=stable();assert c2['data']==c['data'] and s2['data']==s['data'];out['actual_identity_recheck_passed']=True;out['credential_public_JWK_match']=True;out['observed_journal_epoch_after']=original_journal(v2)['owner']['epoch']
except Exception as e:out['error_type']=type(e).__name__
out['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=r/'readonly-native-cap-memory-receipt.json';p.write_text(json.dumps(out,indent=2)+'\n');os.chmod(p,0o600)
