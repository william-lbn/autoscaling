#define _GNU_SOURCE
#include <errno.h>
#include <grp.h>
#include <linux/capability.h>
#include <linux/sched.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>
static void fail(const char *s) { fprintf(stderr,"V2_USERNS_WORKER_FAIL %s errno=%d\n",s,errno);exit(90); }
static void child_done(long p) { int st=0;if(p==0){_exit(0);}if(p<0||waitpid((pid_t)p,&st,0)!=p||!WIFEXITED(st)||WEXITSTATUS(st)!=0){fail("dedicated child Wait");} }
static void caps_zero(void) {
 FILE *f=fopen("/proc/self/status","r");char line[256];int caps=0,nnp=0,sec=0;if(!f){fail("status");}
 while(fgets(line,sizeof(line),f)) {
  if(strncmp(line,"Cap",3)==0&&(strstr(line,"Inh:")||strstr(line,"Prm:")||strstr(line,"Eff:")||strstr(line,"Bnd:")||strstr(line,"Amb:"))){unsigned long long v=1;char *p=strchr(line,':');if(!p||sscanf(p+1,"%llx",&v)!=1||v!=0){fail("cap sets not zero");}caps++;}
  if(strncmp(line,"NoNewPrivs:",11)==0){int v=-1;if(sscanf(line+11,"%d",&v)!=1||v!=0){fail("NNP must remain0");}nnp++;}
  if(strncmp(line,"Seccomp:",8)==0){int v=-1;if(sscanf(line+8,"%d",&v)!=1||v!=0){fail("worker must be unfiltered");}sec++;}
 }
 fclose(f);if(caps!=5||nnp!=1||sec!=1){fail("status fields");}
}
int main(int argc,char **argv) {
 struct utsname u;struct __user_cap_header_struct h={_LINUX_CAPABILITY_VERSION_3,0};struct __user_cap_data_struct d[2]={{0}};
 if(argc!=2||uname(&u)||strcmp(u.release,"6.12.26-william-normal-split-metadata-v1-c67181104ec1")||getuid()!=0||geteuid()!=0||getpid()==1){fail("dedicated exact18c Guest only");}
 for(int i=0;i<=40;i++){if(prctl(PR_CAPBSET_DROP,i,0,0,0)){fail("drop bounding");}}
 if(prctl(PR_CAP_AMBIENT,PR_CAP_AMBIENT_CLEAR_ALL,0,0,0)||setgroups(0,NULL)||setresgid(1000,1000,1000)||setresuid(1000,1000,1000)||syscall(SYS_capset,&h,d)){fail("dedicated nonroot drop");}
 if(getuid()!=1000||geteuid()!=1000||getgid()!=1000||getegid()!=1000){fail("uid gid");}caps_zero();
 if(strcmp(argv[1],"before")==0){if(syscall(SYS_unshare,CLONE_NEWUSER)){fail("positive before zero");}puts("V2_USERNS_BEFORE_POSITIVE UID1000 fiveCaps0 NNP0 Seccomp0 actual unshare NEWUSER accepted");return 0;}
 if(strcmp(argv[1],"after")){fail("fixed worker mode");}
 errno=0;if(syscall(SYS_unshare,CLONE_NEWUSER)!=-1||errno!=ENOSPC){fail("unshare not denied by zero");}
 errno=0;long p=syscall(SYS_clone,(unsigned long)(CLONE_NEWUSER|SIGCHLD),0,0,0,0);if(p>=0){child_done(p);fail("clone NEWUSER unexpectedly accepted");}if(errno!=ENOSPC){fail("clone wrong denial");}
 struct clone_args a;memset(&a,0,sizeof(a));a.flags=CLONE_NEWUSER;a.exit_signal=SIGCHLD;errno=0;p=syscall(SYS_clone3,&a,sizeof(a));if(p>=0){child_done(p);fail("clone3 NEWUSER unexpectedly accepted");}if(errno!=ENOSPC){fail("clone3 wrong denial");}
 child_done(fork());caps_zero();puts("V2_USERNS_AFTER_NEGATIVE UID1000 fiveCaps0 NNP0 Seccomp0 unshare/clone/clone3 NEWUSER ENOSPC; plainfork actual Wait0");return 0;
}
