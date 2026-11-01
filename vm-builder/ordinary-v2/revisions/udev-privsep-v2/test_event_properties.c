/* TEST_ONLY C selector/CPU raw adaptation boundaries; never proves Root Source. */
#define _GNU_SOURCE
#include "creator-ipc.c"
#include "emitter-client.c"
#include <stdio.h>
static void check(int yes,const char *name) {
    if(!yes){fprintf(stderr,"C3_UNIT_FAIL %s\n",name);exit(1);}
}
static void row(const char *root,const char *name) {
    printf("{\"root\":\"%s\",\"case\":\"%s\",\"status\":\"PASS\"}\n",root,name);
}
static void select_case(const char *name,const char *sub,const char *sys,const char *port,int valid,uint16_t expected_kind,uint32_t expected_index) {
    uint16_t kind=0;uint32_t index=0;
    int rc=neon_device_selector(sub,sys,port,&kind,&index);
    check(valid?rc==0&&kind==expected_kind&&index==expected_index:rc<0,name);
    row(valid?"NEON-U-C-UDEV3-001":"NEON-U-C-UDEV3-002",name);
}
#define BODY(path,sub,alias) "ACTION=add\0DEVPATH=" path "\0MODALIAS=" alias "\0SEQNUM=22\0SUBSYSTEM=" sub
#define BAD(name,path,sub,alias) {BODY(path,sub,alias),sizeof(BODY(path,sub,alias)),name}
int main(void) {
    setbuf(stdout,NULL);
    select_case("original-log","virtio-ports","vport0p1","tech.neon.log.0",1,1,0);
    select_case("memory-zero","memory","memory0",NULL,1,2,0);
    select_case("memory-twelve","memory","memory12",NULL,1,2,12);
    select_case("memory-uint32-max","memory","memory4294967295",NULL,1,2,UINT32_MAX);
    select_case("static-cpu","cpu","cpu0",NULL,1,3,0);
    select_case("static-pci","pci","0000:00:00.0",NULL,1,3,0);
    select_case("static-acpi","acpi","LNXSYSTM:00",NULL,1,3,0);
    select_case("non-log-virtio","virtio-ports","vport0p0","tech.neon.control.0",1,3,0);
    select_case("missing-subsystem",NULL,"memory0",NULL,0,0,0);
    select_case("empty-subsystem","","cpu0",NULL,0,0,0);
    select_case("missing-memory-name","memory",NULL,NULL,0,0,0);
    select_case("empty-memory-index","memory","memory",NULL,0,0,0);
    select_case("memory-leading-zero","memory","memory01",NULL,0,0,0);
    select_case("memory-plus","memory","memory+1",NULL,0,0,0);
    select_case("memory-negative","memory","memory-1",NULL,0,0,0);
    select_case("memory-suffix","memory","memory1junk",NULL,0,0,0);
    select_case("memory-overflow","memory","memory4294967296",NULL,0,0,0);
    char out[NEON_IPC_MAX],wire[NEON_IPC_MAX];size_t len=0,wire_len=0;uint32_t count=0,wire_count=0;
    const char cpu[]=BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86,ven0000fam0006mod0006:feature:,0001\n");
    char original[sizeof cpu];memcpy(original,cpu,sizeof cpu);
    check(neon_canonical(cpu,sizeof cpu,out,&len,&count,0)<0,"strict original codec still rejects LF");
    check(!neon_device_canonical(cpu,sizeof cpu,out,&len,&count)&&count==5&&len==sizeof(cpu)-1,"raw CPU normalization");
    check(!memcmp(original,cpu,sizeof cpu),"original raw bytes unchanged");
    const char expected[]=BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86,ven0000fam0006mod0006:feature:,0001");
    check(!neon_canonical(expected,sizeof expected,wire,&wire_len,&wire_count,0)&&wire_len==len&&!memcmp(wire,out,len),"exact independent CPU expected bytes");
    check(!neon_canonical(out,len,wire,&wire_len,&wire_count,1),"normalized wire strictly canonical");
    row("NEON-U-C-UDEV3-003","single-terminal-LF-original-and-wire");
    const struct {const char *body;size_t len;const char *name;} bad[]={
        BAD("wrong-subsystem","/devices/system/cpu/cpu0","pci","cpu:type:x86\n"),
        BAD("wrong-path","/devices/test","cpu","cpu:type:x86\n"),
        BAD("cpu-leading-zero","/devices/system/cpu/cpu00","cpu","cpu:type:x86\n"),
        BAD("cpu-plus","/devices/system/cpu/cpu+1","cpu","cpu:type:x86\n"),
        BAD("cpu-overflow","/devices/system/cpu/cpu4294967296","cpu","cpu:type:x86\n"),
        BAD("two-LF","/devices/system/cpu/cpu0","cpu","cpu:type:x86\n\n"),
        BAD("interior-LF","/devices/system/cpu/cpu0","cpu","cpu:type:x\n86"),
        BAD("wrong-alias-prefix","/devices/system/cpu/cpu0","cpu","pci:type:x86\n"),
        {cpu,sizeof(cpu)-1,"missing-final-NUL"},
        {BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0MODALIAS=cpu:type:forged\n",sizeof(BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0MODALIAS=cpu:type:forged\n"),"duplicate-alias"},
        {BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0DEVPATH=/devices/system/cpu/cpu0",sizeof(BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0DEVPATH=/devices/system/cpu/cpu0"),"duplicate-path"},
        {BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0SUBSYSTEM=cpu",sizeof(BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0SUBSYSTEM=cpu"),"duplicate-subsystem"},
        {BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0UNKNOWN=bad\n",sizeof(BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0UNKNOWN=bad\n"),"other-LF"},
        {BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0DRIVER=\tbad",sizeof(BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0DRIVER=\tbad"),"other-control"},
        {BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0DRIVER=\xc0\x80",sizeof(BODY("/devices/system/cpu/cpu0","cpu","cpu:type:x86\n") "\0DRIVER=\xc0\x80"),"other-invalid-UTF8"}
    };
    for(size_t i=0;i<sizeof bad/sizeof bad[0];i++) {
        check(neon_device_canonical(bad[i].body,bad[i].len,out,&len,&count)<0,bad[i].name);
        row("NEON-U-C-UDEV3-004",bad[i].name);
    }
    const char ordinary[]="SUBSYSTEM=pci\0SEQNUM=2\0DEVPATH=/devices/test\0ACTION=add";
    check(!neon_device_canonical(ordinary,sizeof ordinary,out,&len,&count)&&count==4,"ordinary raw strict sort");
    check(!neon_canonical(out,len,wire,&wire_len,&wire_count,1),"ordinary canonical wire");
    const char duplicate[]="A=1\0A=2";
    check(neon_device_canonical(duplicate,sizeof duplicate,out,&len,&count)<0,"ordinary duplicate rejects");
    row("NEON-U-C-UDEV3-005","ordinary-codec-invariants");
    neon_original_main=getpid();
    const struct {uint16_t kind,action;uint32_t index;uint64_t seq;uint32_t count;const char *props;size_t len;const char *name;} invalid[]={
        {0,1,0,1,4,ordinary,sizeof ordinary,"kind-zero"},{4,1,0,1,4,ordinary,sizeof ordinary,"kind-unknown"},
        {3,1,1,1,4,ordinary,sizeof ordinary,"static-index"},{1,1,1,1,4,ordinary,sizeof ordinary,"log-index"},
        {3,0,0,1,4,ordinary,sizeof ordinary,"action-zero"},{3,9,0,1,4,ordinary,sizeof ordinary,"action-unknown"},
        {3,1,0,0,4,ordinary,sizeof ordinary,"seq-zero"},{3,1,0,1,0,ordinary,sizeof ordinary,"count-zero"},
        {3,1,0,1,65,ordinary,sizeof ordinary,"count-overflow"},{3,1,0,1,4,NULL,sizeof ordinary,"null-properties"},
        {3,1,0,1,4,ordinary,0,"empty-properties"},{3,1,0,1,4,ordinary,NEON_EVENT_MAX+1,"oversized-properties"}
    };
    for(size_t i=0;i<sizeof invalid/sizeof invalid[0];i++) {
        check(neon_root_emit(invalid[i].kind,invalid[i].action,invalid[i].index,invalid[i].seq,invalid[i].props,invalid[i].len,invalid[i].count)==-EINVAL&&neon_emitter_fd==-1,invalid[i].name);
        row("NEON-U-C-UDEV3-006",invalid[i].name);
    }
    puts("C3_CODEC_UNIT_PASS_NOT_SOURCE");return 0;
}
