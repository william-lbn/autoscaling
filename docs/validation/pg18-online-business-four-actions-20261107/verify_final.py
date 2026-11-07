from pathlib import Path as P
import base64, hashlib, json, sys, subprocess, datetime
sys.path.insert(0,str(P(__file__).parent.parent))
from durable_case import SQL, Witness, ledger, quorum, wal_urls
def execute(command,env,fds,params,binding,out):
    original = P(__file__).parent.parent
    accepted = json.loads((original/'ACCEPTED.PUBLIC.json').read_text())
    assert accepted['writer_exit'] == accepted['load_exit'] == 0
    vm, model = ledger(params['current_VMUID'])
    assert not model.get('active') and not model.get('service_closure')
    assert model['high_water'] == accepted['actions'][-1]['key']
    runner = json.loads(subprocess.check_output(['kubectl','-n','neon','get','cm',
        'dsga-s-626047190647e45659154d4e39a3469dad9bbb0c2668e4754f20ecc7','-o','json'],timeout=12))
    publication = json.loads(runner['data']['runner.json'])
    assert publication['state']['pending'] is None
    assert publication['actual']['requested_bytes'] == publication['actual']['actual_bytes'] == '7516192768'
    assert publication['state']['high_water'] == model['high_water']
    sql = SQL(command,env,fds)
    try:
        witness = Witness(sql,params,binding,wal_urls(binding),out)
        assert witness.boot == accepted['postmaster']
        witness.n = accepted['total_commits']
        data = witness.data(sql)
        assert data['mark_ids'] == accepted['final_reconnect']['data']['mark_ids']
        wal = quorum(witness.urls,accepted['last']['SQL']['after_commit'])
    finally:
        status = sql.close()
    assert status == 0
    (out/'actual.PUBLIC.json').write_text(json.dumps({'passed':True,'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'PG_major':18,'UID':params['current_VMUID'],'PG_boot':accepted['pg_boot'],'owner_epoch':model['owner']['epoch'],
        'data':data,'safekeepers':wal,'runner_pending':None,'runner_actual':publication['actual'],
        'source_accepted_sha256':hashlib.sha256((original/'ACCEPTED.PUBLIC.json').read_bytes()).hexdigest()},indent=2))
