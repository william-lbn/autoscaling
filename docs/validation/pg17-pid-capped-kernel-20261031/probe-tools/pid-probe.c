#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/capability.h>
#include <linux/sched.h>
#include <stdint.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static void die(const char *stage) {
    fprintf(stderr, "PG17_PID_PROBE_FAIL stage=%s errno=%d (%s)\n", stage, errno, strerror(errno));
    exit(2);
}
static unsigned read_uint(const char *path) {
    FILE *f=fopen(path,"r"); unsigned n; char extra;
    if (!f || fscanf(f,"%u %c",&n,&extra)!=1) die("read-kernel-uint");
    if (fclose(f)) die("close-kernel-uint");
    return n;
}
static int put_uint(const char *path,unsigned n) {
    char b[32]; int len=snprintf(b,sizeof(b),"%u\n",n);
    int fd=open(path,O_WRONLY|O_CLOEXEC);
    if(fd<0) return -1;
    ssize_t size=write(fd,b,(size_t)len); int e=errno; int c=close(fd);
    if(size!=len){errno=e;return -1;} if(c){return -1;} return 0;
}
static void wait_clean(pid_t pid) {
    int status; pid_t n;
    do { n=waitpid(pid,&status,0); } while(n<0&&errno==EINTR);
    if(n!=pid || !WIFEXITED(status) || WEXITSTATUS(status)!=0) die("strict-child-wait");
}
static unsigned proc_self_host_id(void) {
    FILE *f=fopen("/proc/self/stat","r"); unsigned p;
    if(!f || fscanf(f,"%u",&p)!=1) die("read-proc-host-pid");
    if(fclose(f)) die("close-proc-host-pid");
    return p;
}
static int capped_forks(unsigned cap,unsigned count) {
    unsigned max=0;
    for(unsigned i=0;i<count;i++) {
        pid_t child=fork();
        if(child<0) return 1;
        if(child==0) {
            unsigned host=proc_self_host_id();
            _exit(getpid()>0 && (unsigned)getpid()<cap && host>0 && host<cap ? 0 : 7);
        }
        if((unsigned)child>=cap) return 2;
        if((unsigned)child>max) max=(unsigned)child;
        wait_clean(child);
    }
    printf("PG17_PID_FORK={\"iterations\":%u,\"visible_max_child_pid\":%u,\"exclusive_cap\":%u,\"all_strict_wait\":true}\n",count,max,cap);
    fflush(stdout);return 0;
}
static pid_t clone_set(int flags,pid_t *ids,size_t count) {
    struct clone_args args={0};
    args.flags=(uint64_t)flags;args.exit_signal=SIGCHLD;
    args.set_tid=(uint64_t)(uintptr_t)ids;args.set_tid_size=count;
    return (pid_t)syscall(SYS_clone3,&args,sizeof(args));
}
static void set_tid_test(unsigned cap) {
    struct __user_cap_header_struct h={.version=_LINUX_CAPABILITY_VERSION_3,.pid=0};
    struct __user_cap_data_struct c[2]={{0}};
    if(syscall(SYS_capget,&h,c)) die("capget");
    int admin=(c[0].effective&(1U<<CAP_SYS_ADMIN))!=0;
    int restore=(c[1].effective&(1U<<(CAP_CHECKPOINT_RESTORE-32)))!=0;
    if(!admin&&!restore) die("missing-real-settid-capability");
    pid_t good=(pid_t)(cap-1);
    pid_t p=clone_set(0,&good,1);
    if(p==0) _exit((unsigned)getpid()==cap-1 ? 0:8);
    if(p!=good) die("valid-settid-precondition");
    wait_clean(p);
    pid_t bad=(pid_t)cap;errno=0;p=clone_set(0,&bad,1);int e=errno;
    if(p==0) _exit(9);
    if(p>0){wait_clean(p);die("settid-cap-created-child");}
    if(e!=EINVAL) {errno=e;die("settid-cap-not-EINVAL");}
    pid_t nested_ids[2]={1,(pid_t)(cap-1)};
    p=clone_set(CLONE_NEWPID,nested_ids,2);
    if(p==0) {
        if(getpid()!=1 || proc_self_host_id()!=cap-1) _exit(10);
        /* Every child also allocates an initial-namespace ancestor ID. */
        _exit(capped_forks(cap,240));
    }
    if(p!=nested_ids[1]) die("valid-nested-settid-precondition");
    wait_clean(p);
    nested_ids[1]=(pid_t)cap;errno=0;p=clone_set(CLONE_NEWPID,nested_ids,2);e=errno;
    if(p==0) _exit(11);
    if(p>0){wait_clean(p);die("ancestor-cap-created-child");}
    if(e!=EINVAL) {errno=e;die("ancestor-cap-not-EINVAL");}
    printf("PG17_PID_SETTID={\"cap_sys_admin_effective\":%s,\"cap_checkpoint_restore_effective\":%s,\"valid_same_namespace\":true,\"exclusive_cap_errno\":%d,\"valid_nested_namespace\":true,\"ancestor_cap_errno\":%d,\"nested_forks_strict\":true}\n",admin?"true":"false",restore?"true":"false",EINVAL,EINVAL);
}
int main(int argc,char **argv) {
    if(argc!=2 || getuid()!=0 || geteuid()!=0) die("explicit-expected-or-root");
    unsigned expected=(unsigned)strtoul(argv[1],NULL,10);
    const char *fact="/proc/sys/kernel/neonvm_pid_max_limit";
    const char *live="/proc/sys/kernel/pid_max";
    unsigned hard=read_uint(fact), initial=read_uint(live);
    if(hard!=expected) die("hard-source-mismatch");
    if(expected && initial!=expected) die("CPU-or-late-boot-expanded");
    errno=0;int w=put_uint(fact,0),e=errno;
    if(w==0 || read_uint(fact)!=expected) die("hard-source-clear-accepted");
    printf("PG17_PID_READONLY_ZERO errno=%d\n",e);
    errno=0;w=put_uint(fact,1024);e=errno;
    if(w==0 || read_uint(fact)!=expected) die("hard-source-change-accepted");
    printf("PG17_PID_READONLY_1024 errno=%d\n",e);
    if(!expected) {
        if(initial<=512 || put_uint(live,4096) || read_uint(live)!=4096 ||
           put_uint(live,initial) || read_uint(live)!=initial) die("default-behavior-control");
        if(capped_forks(initial,2)) die("default-fork");
        printf("PG17_PID_PROBE={\"scope\":\"isolated-kvm-kernel-component-only\",\"hard_limit\":0,\"initial_pid_max\":%u,\"default_can_set_4096\":true,\"vm_shrink_permit\":false,\"pass\":true}\n",initial);
        return 0;
    }
    if(put_uint(live,expected) || read_uint(live)!=expected) die("same-cap-write");
    unsigned invalids[2]={expected+1,4096};
    for(unsigned i=0;i<2;i++) {
        if(invalids[i]<=expected) continue;
        errno=0;w=put_uint(live,invalids[i]);e=errno;
        if(w==0 || e!=EINVAL || read_uint(live)!=expected) die("runtime-expansion-not-rejected");
        printf("PG17_PID_RUNTIME_REJECT value=%u errno=%d\n",invalids[i],e);
    }
    if(expected>301) {
        if(put_uint(live,301) || read_uint(live)!=301) die("legal-lower");
        if(put_uint(live,expected) || read_uint(live)!=expected) die("legal-restore-to-original");
        errno=0;w=put_uint(live,expected+1);e=errno;
        if(w==0 || e!=EINVAL || read_uint(live)!=expected || read_uint(fact)!=expected) die("restore-above-original");
    }
    if(capped_forks(expected,expected==512 ? 700:4)) die("bounded-fork-loop");
    if(expected==512) set_tid_test(expected);
    printf("PG17_PID_PROBE={\"scope\":\"isolated-kvm-kernel-component-only\",\"hard_limit\":%u,\"initial_pid_max\":%u,\"readonly_fact\":true,\"runtime_expand_rejected\":true,\"normal_init_and_fork\":true,\"legal_lower_restore_checked\":%s,\"vm_shrink_permit\":false,\"pass\":true}\n",hard,initial,expected>301?"true":"false");
    return 0;
}
