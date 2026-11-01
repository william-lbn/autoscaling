
from pathlib import Path
import re,json,hashlib,base64,collections
p=Path('/data/neon-build/validation/normal-role-authority-declaration1-native-20261101T051523Z-afab49');out=p/'independent-reevaluation-v1';out.mkdir(exist_ok=False)
h=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
raw=p/'native-run/console.log';text=raw.read_text(errors='strict');old=json.loads((p/'native-run/result.json').read_text());container=json.loads((p/'native-run/container-actual.json').read_text())
ids=re.findall(r'^(NEON-S-SYSTEM-ROLE-SERVICE-(?:00[1-7]|011)) PASS[^\r\n]*',text,re.M);counts=collections.Counter(ids);expected=[f'NEON-S-SYSTEM-ROLE-SERVICE-{n:03d}'for n in list(range(1,8))+[11]]
notes=re.search(r'KERNEL_NOTES_BEGIN\r?\n([\s\S]*?)KERNEL_NOTES_END',text);actual=base64.b64decode(''.join(notes[1].split()),validate=True)
checks={
'original_report_is_preserved_FAIL':old['pass']is False,
'Guest_single_zero':re.findall(r'^ROLE_SERVICE_GUEST_EXIT=(\d+)\r?$',text,re.M)==['0'],
'initialRoot_single_zero':re.findall(r'^ROLE_SERVICE_INITIAL_EXIT=(\d+)\r?$',text,re.M)==['0'],
'actual_QEMU_container_zero_not_OOM':container['State']['ExitCode']==0 and not container['State']['OOMKilled'] and not container['State']['Running'],
'original_QMP_query_KVM_exact':old['actual_kvm']=={'enabled':True,'present':True},
'actual_notes_equal_original_kernel_bytes':actual==Path('/data/neon-build/validation/normal-split-kernel-20261030T224843Z/export/kernel-notes.bin').read_bytes(),
'all_expected_stable_IDs_and_only_those':set(ids)==set(expected),
'first_7_checkpoints_exactly_once':all(counts[n]==1 for n in expected[:-1]),
'legacy_repeated_011_only':counts[expected[-1]]==2 and len(ids)==9,
'no_panic_or_failure_or_legacy_refusal':all(n not in text for n in ['Kernel panic - not syncing:','ROLE_AUTHORITY_DEV_LEGACY_SERVE_REFUSED','ROLE_AUTHORITY_DEV_LEGACY_HTTP_TIMEOUT','ROLE_SERVICE_CLIENT_REFUSED','--- FAIL:','ROLE_SERVICE_BROKER_REFUSED','ROLE_SERVICE_GATE_REFUSED']),
'same_compiled_Go_ELF_as_declaration2':h(p/'export/service.test')==h(Path('/data/neon-build/validation/normal-role-authority-v2-native-build-20261101T051423Z-62cf67/export/service.test'))}
assert all(checks.values()),checks
result={'status':'PASS_COMPONENT_ONLY_REEVALUATED','pass':True,'checks':checks,'case_ID_counts':dict(sorted(counts.items())),'native_roots':1,'stable_IDs':expected,'raw_SHA256':{'console':h(raw),'original_FAIL_result':h(p/'native-run/result.json'),'original_driver':h(p/'run-native.py'),'actual_container':h(p/'native-run/container-actual.json'),'Go_ELF':h(p/'export/service.test')},'reason':'原检查器对PASS行按列表恰好一次比较。TEST_ONLY legacy observer在marker已存在时仍打印同一011；本收据仅将重复011视为同一稳定检查点，其他检查不放宽，不重跑Guest，不覆盖原FAIL。重复诊断本身不证明服务重生次数。','actual_KVM_fact_source':'原driver真实QMP query-kvm结果；QEMU原实际容器亦已退出0。未新造QMP实测。','source_fixture':True,'actual_CLI_executed':False,'FS_protocol':1,'PG_executed':False,'normal_authorized':False,'E':0}
(out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
(out/'README.txt').write_text('原systemd任务仍为exit-code/1；原result.json仍为FAIL。此目录是独立原始收据重评，不覆盖任何旧文件。\n旧声明1兼容性检查是同一native根的独立环境模式，不新增8个S根。\n')
print(json.dumps({'path':str(out),'result_SHA256':h(out/'result.json'),'raw_FAIL_SHA256':h(p/'native-run/result.json'),'checks':checks},ensure_ascii=False))
