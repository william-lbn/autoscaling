#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sched.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* 仅真实Guest内核组件探针；counter不是RSS，不发Root/PG/VM缩容资格。 */
static const char *base="/sys/fs/cgroup/counter-probe";
static void fail(const char *s){fprintf(stderr,"PG17_COUNTER_FAIL stage=%s errno=%d\n",s,errno);exit(2);}
static void path(char *out,size_t n,const char *group,const char *file){int x=snprintf(out,n,"%s/%s%s%s",base,group,*group?"/":"",file);if(x<0||(size_t)x>=n)fail("path");}
static void put(const char *p,const char *s){int fd=open(p,O_WRONLY|O_CLOEXEC);if(fd<0)fail(p);size_t size=strlen(s),done=0;while(done<size){ssize_t x=write(fd,s+done,size-done);if(x<0&&errno==EINTR)continue;if(x<=0)fail("write");done+=(size_t)x;}if(close(fd))fail("close");}
static uint64_t number(const char *p){FILE *f=fopen(p,"r");unsigned long long x;char extra;if(!f||fscanf(f,"%llu %c",&x,&extra)!=1)fail(p);if(fclose(f))fail("close-read");return(uint64_t)x;}
static uint64_t get(const char *g,const char *f){char p[512];path(p,sizeof(p),g,f);return number(p);}
static void set(const char *g,const char *f,const char *v){char p[512];path(p,sizeof(p),g,f);put(p,v);}
static void mkdir_one(const char *p){if(mkdir(p,0755))fail(p);}
static void group(const char *g){char p[512];path(p,sizeof(p),g,"");mkdir_one(p);}
static void join(const char *g){char pid[32];snprintf(pid,sizeof(pid),"%ld",(long)getpid());set(g,"cgroup.procs",pid);}
static void no_oom(const char *g){char p[512],key[64];unsigned long long v;path(p,sizeof(p),g,"memory.events.local");FILE *f=fopen(p,"r");if(!f)fail("events");while(fscanf(f,"%63s %llu",key,&v)==2){if((!strcmp(key,"oom")||!strcmp(key,"oom_kill")||!strcmp(key,"oom_group_kill")||!strcmp(key,"max")||!strcmp(key,"high"))&&v)fail("unexpected-memory-event");}if(fclose(f))fail("events-close");}
static void init_groups(void){put("/sys/fs/cgroup/cgroup.subtree_control","+memory +pids");mkdir_one(base);set("","cgroup.subtree_control","+memory +pids");set("","memory.swap.max","0");group("private");group("arrays");set("private","memory.swap.max","0");set("arrays","memory.swap.max","0");}
static void child_load(const char *g,unsigned slot,unsigned loops,size_t maxbytes){
    cpu_set_t cpus;CPU_ZERO(&cpus);CPU_SET(slot%2,&cpus);if(sched_setaffinity(0,sizeof(cpus),&cpus))fail("affinity");join(g);
    for(unsigned i=0;i<loops;i++){
        size_t pages=(maxbytes/4096)-(i%17);size_t size=pages*4096;
        volatile unsigned char *p=mmap(NULL,size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);if(p==MAP_FAILED)fail("mmap");
        for(size_t off=0;off<size;off+=4096)p[off]=(unsigned char)(slot+i+1);
        if((i%8)==0){if(madvise((void*)p,size,MADV_DONTNEED))fail("madvise");for(size_t off=0;off<size;off+=4096)p[off]=(unsigned char)(i+3);}
        if(munmap((void*)p,size))fail("munmap");
    }
    _exit(0);
}
static uint64_t load_phase(unsigned pworkers,unsigned aworkers,unsigned loops,uint64_t limit){
    pid_t ids[16];unsigned n=pworkers+aworkers,finished=0;int reaped[16]={0};uint64_t peak=0;
    if(n>16)fail("worker-count");
    for(unsigned i=0;i<n;i++){ids[i]=fork();if(ids[i]<0)fail("fork");if(ids[i]==0)child_load(i<pworkers?"private":"arrays",i,loops,4*1024*1024);}
    while(finished<n){uint64_t cur=get("","memory.current");if(cur>peak)peak=cur;if(cur>limit)fail("parent-counter-exceeded-explicit-envelope");
        for(unsigned i=0;i<n;i++){if(reaped[i])continue;int st;pid_t x=waitpid(ids[i],&st,WNOHANG);if(x<0)fail("waitpid");if(x==ids[i]){if(!WIFEXITED(st)||WEXITSTATUS(st))fail("worker-terminal");reaped[i]=1;finished++;}}
        usleep(1000);
    }
    no_oom("");no_oom("private");no_oom("arrays");return peak;
}
static int pressure(void){
    init_groups();set("","memory.max","54525952");set("private","memory.max","25165824");set("arrays","memory.max","25165824");
    uint64_t first=load_phase(4,4,256,54525952),before=get("","memory.current");
    /* 空arrays且无新出生者时先降其真实cap，再降同一父cap；只作组件机制验证。 */
    if(get("arrays","memory.current")>12582912||before>41943040)fail("shrink-readback-headroom");
    set("arrays","memory.max","12582912");set("","memory.max","41943040");
    if(get("arrays","memory.max")!=12582912||get("","memory.max")!=41943040||get("private","memory.max")!=25165824)fail("cap-readback");
    uint64_t second=load_phase(4,2,256,41943040);
    struct stat p,a,b;if(stat(base,&p))fail("stat-parent");char q[512];path(q,sizeof(q),"private","");if(stat(q,&a))fail("stat-private");path(q,sizeof(q),"arrays","");if(stat(q,&b))fail("stat-arrays");
    printf("PG17_COUNTER_PROBE={\"scope\":\"kernel-counter-component-only\",\"mode\":\"pressure\",\"parent_inode\":%ju,\"private_inode\":%ju,\"arrays_inode\":%ju,\"phase1_workers\":8,\"phase2_workers\":6,\"loops_per_worker\":256,\"all_children_strictly_reaped\":true,\"parent_limit_before\":54525952,\"parent_limit_after\":41943040,\"private_limit_fixed\":25165824,\"arrays_limit_after\":12582912,\"counter_margin_bytes\":4194304,\"phase1_observed_counter_peak\":%"PRIu64",\"phase2_observed_counter_peak\":%"PRIu64",\"memory_events_zero\":true,\"vm_shrink_permit\":false,\"all_sources_closed\":false,\"pass\":true}\n",(uintmax_t)p.st_ino,(uintmax_t)a.st_ino,(uintmax_t)b.st_ino,first,second);return 0;
}
static int irq_work(void){
    init_groups();set("","memory.max","67108864");set("private","memory.max","33554432");set("arrays","memory.max","33554432");
    /* 正常uncharge与stock切换产生真实入口，之后独立parent=0制造祖先失败回滚。 */
    (void)load_phase(1,1,8,67108864);
    group("failure");set("failure","cgroup.subtree_control","+memory");group("failure/middle");set("failure/middle","cgroup.subtree_control","+memory");group("failure/middle/leaf");set("failure","memory.max","0");
    puts("PG17_IRQ_FAILURE_PHASE_BEGIN");fflush(stdout);unsigned observed=0;
    for(unsigned i=0;i<4;i++){
        pid_t p=fork();if(p<0)fail("irq-fork");if(p==0){join("failure/middle/leaf");volatile unsigned char *x=mmap(NULL,4*1024*1024,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);if(x==MAP_FAILED)_exit(errno==ENOMEM?7:9);for(size_t z=0;z<4*1024*1024;z+=4096)x[z]=1;_exit(8);}
        int st;pid_t w;do{w=waitpid(p,&st,0);}while(w<0&&errno==EINTR);if(w!=p)fail("irq-wait");if(!((WIFSIGNALED(st)&&WTERMSIG(st)==SIGKILL)||(WIFEXITED(st)&&WEXITSTATUS(st)==7)))fail("expected-failure-child");observed++;
    }
    printf("PG17_COUNTER_PROBE={\"scope\":\"kernel-counter-component-only\",\"mode\":\"irq-trigger-workload\",\"expected_failed_children\":%u,\"strictly_reaped\":true,\"vm_shrink_permit\":false,\"all_sources_closed\":false,\"pass\":true}\n",observed);return 0;
}
int main(int argc,char **argv){if(argc!=2||getuid()!=0)fail("arguments-root");if(!strcmp(argv[1],"pressure"))return pressure();if(!strcmp(argv[1],"irq"))return irq_work();fail("mode");return 2;}
