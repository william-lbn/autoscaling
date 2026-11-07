from pathlib import Path as P
import concurrent.futures, datetime, hashlib, json, os, re, subprocess
os.umask(0o077)
root = P(__file__).parent
accepted = json.loads((root/'ACCEPTED.PUBLIC.json').read_text())
assert len(accepted['actions']) == 4 and accepted['writer_exit'] == accepted['load_exit'] == 0
start = json.loads((root/'write-intent.PUBLIC.json').read_text())['at']
directory = root/'组件日志-完成四项'
directory.mkdir(mode=0o700)
pods = json.loads(subprocess.check_output(['kubectl','get','pods','-A','-o','json'],timeout=20))['items']
selected = [pod for pod in pods if pod['status']['phase'] == 'Running'
    and (pod['metadata']['namespace'] in ('neon','neonvm-system')
         or pod['metadata']['name'].startswith(('autoscaler-agent-','autoscale-scheduler-')))]
def collect(job):
    pod, container = job
    namespace, name = pod['metadata']['namespace'], pod['metadata']['name']
    filename = namespace+'--'+name+'--'+container+'.private.log'
    result = subprocess.run(['kubectl','--request-timeout=15s','-n',namespace,'logs',name,
        '-c',container,'--since-time='+start,'--timestamps=true','--limit-bytes=2097152'],
        capture_output=True,timeout=25)
    (directory/filename).write_bytes(result.stdout)
    (directory/(filename+'.stderr')).write_bytes(result.stderr)
    lines = result.stdout.decode(errors='replace').splitlines()
    signals = [line for line in lines if re.search(r'(?i)(\"level\"\s*:\s*\"(warn|error|fatal)\"|\b(ERROR|FATAL|PANIC):)',line)]
    status = next(x for x in pod['status']['containerStatuses'] if x['name']==container)
    return {'namespace':namespace,'pod':name,'pod_uid':pod['metadata']['uid'],
        'node':pod['spec']['nodeName'],'container':container,'image':status['image'],
        'image_id':status.get('imageID'),'ready':status['ready'],'restarts':status['restartCount'],
        'exit':result.returncode,'bytes':len(result.stdout),'sha256':hashlib.sha256(result.stdout).hexdigest(),
        'log_file':filename,'error_warning_count':len(signals),'error_warning_file':filename+'.signals.private.json',
        'signals':signals}
jobs = [(pod,container['name']) for pod in selected for container in pod['spec']['containers']]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(collect,jobs))
for item in results:
    signals = item.pop('signals')
    (directory/item['error_warning_file']).write_text(json.dumps(signals,ensure_ascii=False,indent=2))
index = {'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'since':start,
    'count':len(results),'all_collected':all(item['exit']==0 for item in results),'components':results}
(directory/'index.PUBLIC.json').write_text(json.dumps(index,ensure_ascii=False,indent=2))
assert index['all_collected']
print(json.dumps({'containers':len(results),'all_collected':True,
    'with_warnings':[{k:item[k]for k in ('namespace','pod','container','error_warning_count')}
        for item in results if item['error_warning_count']]}))
