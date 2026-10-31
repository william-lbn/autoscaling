# 在同一准确生产kernel的隔离KVM中运行；不调用Guest函数、不写内核文本或counter。
import gdb, json, traceback
from pathlib import Path

OUT = Path('/results')
PARENT_OFFSET = 0xe8  # 准确导出的两函数反汇编共同给出的struct parent偏移；仅测试解码。
SITES = {
    'uncharge': {'save':0xffffffff813334bb,'cli':0xffffffff813334be,'sti':0xffffffff81333511},
    'rollback': {'save':0xffffffff81333456,'cli':0xffffffff81333458,'sti':0xffffffff8133347e},
}


def reg(name):
    return int(gdb.parse_and_eval('$'+name)) & ((1 << 64) - 1)


def word(address):
    return int.from_bytes(bytes(gdb.selected_inferior().read_memory(address & ((1 << 64) - 1),8)),'little')


def chain(start, stop=0):
    result=[]
    while start and start!=stop:
        if len(result)>=12 or not(start & (1<<63)) or start%8:
            raise RuntimeError('unexpected real counter chain')
        result.append({'address':hex(start),'usage':word(start)})
        start=word(start+PARENT_OFFSET)
    if start!=stop:
        raise RuntimeError('failure ancestor absent from original counter chain')
    return result


def capture(site_name, wanted_if):
    site=SITES[site_name]
    breakpoint=gdb.Breakpoint('*'+hex(site['save']),type=gdb.BP_HARDWARE_BREAKPOINT,internal=True)
    selected=None
    for attempt in range(200):
        gdb.execute('continue',to_string=True)
        if reg('rip')!=site['save']:
            raise RuntimeError('unexpected stop before actual irq-save')
        if site_name=='uncharge':
            amount=reg('rsi');items=chain(reg('rdi'))
        else:
            amount=reg('r13');items=chain(reg('r15'),word(reg('rax')))
        if len(items)>=2 and amount>0 and all(i['usage']>=amount for i in items):
            selected=items
            break
    breakpoint.delete()
    if selected is None:
        raise RuntimeError('no real multi-ancestor cancellation reached')
    for key,byte in [('save',b'\x9c'),('cli',b'\xfa'),('sti',b'\xfb')]:
        if bytes(gdb.selected_inferior().read_memory(site[key],1))!=byte:
            raise RuntimeError('runtime instruction differs from exact artifact at '+key)
    original_flags=reg('eflags');test_flags=(original_flags&~0x200)|(0x200 if wanted_if else 0)
    before_thread=tuple(gdb.selected_thread().ptid)
    return_address=word(reg('rbp')+8)
    # 只在pushf保存之前控制入口IF；不改任何保存值/函数输入/counter/内核文本。
    # QEMU单步屏蔽外部IRQ投递（含maskable timer IRQ），使IF=1的边界注入不破坏原IRQ-off caller持锁条件；
    # 函数真实返回后立即恢复原caller IF再继续其他CPU。
    gdb.execute('set $eflags = '+hex(test_flags),to_string=True)
    trace=[];cli_seen=False;restore_seen=False;cli_was_zero=False
    for step in range(5000):
        rip=reg('rip');flags=reg('eflags')
        if rip==return_address:
            break
        trace.append({'rip':hex(rip),'if':bool(flags&0x200)})
        gdb.execute('si',to_string=True)
        if tuple(gdb.selected_thread().ptid)!=before_thread:
            raise RuntimeError('step switched CPU/context')
        now=reg('eflags')
        if rip==site['cli']:
            cli_seen=True;cli_was_zero=not bool(now&0x200)
            if not cli_was_zero:raise RuntimeError('actual CLI left IF enabled')
        elif cli_seen and now&0x200:
            if rip==site['sti'] and wanted_if:
                restore_seen=True
            elif not restore_seen:
                raise RuntimeError('IF enabled inside cancellation before exact restore')
    else:
        raise RuntimeError('cancellation did not return within bounded instruction count')
    returned_flags=reg('eflags')
    if not cli_seen or bool(returned_flags&0x200)!=bool(wanted_if):
        raise RuntimeError('real entry IF not preserved at return')
    if bool(wanted_if)!=restore_seen:
        raise RuntimeError('restore branch differs from original saved IF')
    after=[{'address':i['address'],'usage':word(int(i['address'],16))} for i in selected]
    if any(a['usage']!=b['usage']-amount for a,b in zip(after,selected)):
        raise RuntimeError('real ancestor counter delta differs')
    gdb.execute('set $eflags = '+hex((returned_flags&~0x200)|(original_flags&0x200)),to_string=True)
    return {'site':site_name,'entry_if_requested':bool(wanted_if),'natural_entry_if':bool(original_flags&0x200),'entry_if_injected':bool((original_flags^test_flags)&0x200),'original_rflags':hex(original_flags),'test_entry_rflags':hex(test_flags),'returned_rflags':hex(returned_flags),'caller_if_restored_before_continue':bool(reg('eflags')&0x200)==bool(original_flags&0x200),'real_ancestor_count':len(selected),'real_nr_pages':amount,'before':selected,'after':after,'cli_observed_if_zero':cli_was_zero,'exact_restore_branch_seen':restore_seen,'instruction_steps':len(trace),'instructions':trace,'guest_function_called_by_debugger':False,'guest_counter_or_text_written':False,'pass':True}


results=[]
try:
    supported_reply=gdb.execute('maintenance packet qqemu.sstepbits',to_string=True)
    if 'NOIRQ=2' not in supported_reply:
        raise RuntimeError('KVM BLOCKIRQ single-step capability missing: '+supported_reply)
    step_reply=gdb.execute('maintenance packet Qqemu.sstep=0x3',to_string=True)
    if 'OK' not in step_reply:
        raise RuntimeError('QEMU KVM NOIRQ single-step control unavailable: '+step_reply)
    for site_name in ['uncharge','rollback']:
        for enabled in [False,True]:
            value=capture(site_name,enabled);results.append(value)
            (OUT/'irq-boundaries-progress.json').write_text(json.dumps({'status':'RUNNING','results':results},indent=2)+'\n')
    evidence={'id':'PG17-S-IRQCOUNTER-002','scope':'same exact kernel hardware-breakpoint CPU IF boundary injection; not natural-caller census or production Root qualification','status':'PASS_REGISTER_COMPONENT','qemu_single_step_no_irq':True,'qemu_notimer_requested':False,'qemu_sstepbits_actual_reply':supported_reply,'results':results,'counts':{'sites':2,'entry_states_each':2,'passed':4},'kernel_text_modified':False,'counter_arguments_or_memory_modified':False,'vm_shrink_permit':False,'all_sources_closed':False}
    (OUT/'irq-boundaries.json').write_text(json.dumps(evidence,indent=2)+'\n')
    print('PG17_GDB_IRQ_BOUNDARIES='+json.dumps({'status':evidence['status'],'counts':evidence['counts']}),flush=True)
    gdb.execute('detach',to_string=True)
except BaseException as error:
    (OUT/'irq-boundaries.json').write_text(json.dumps({'status':'FAIL_REGISTER_COMPONENT','error':repr(error),'traceback':traceback.format_exc(),'results':results,'vm_shrink_permit':False},indent=2)+'\n')
    print(traceback.format_exc(),flush=True)
    gdb.execute('quit 3')
