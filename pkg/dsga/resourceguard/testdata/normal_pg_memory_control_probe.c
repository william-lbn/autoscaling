/* TEST_ONLY：在真实PG backend的继承filter/cap0下执行最小syscall机制负例。
 * 不向生产扩展注册、不安装或削弱filter、不写宿主/Guest控制值。 */
#define _GNU_SOURCE
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include <errno.h>
#include <fcntl.h>
#include <linux/io_uring.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/ptrace.h>
#include <sys/socket.h>
#include <sys/swap.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>
#ifndef SYS_set_mempolicy_home_node
#define SYS_set_mempolicy_home_node 450
#endif
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(normal_pg_memory_control_probe);
Datum normal_pg_memory_control_probe(PG_FUNCTION_ARGS)
{
 char *name=text_to_cstring(PG_GETARG_TEXT_PP(0));
 void *area=mmap(NULL,4096,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
 long result=-1; int saved; char out[512];
 if(area==MAP_FAILED)ereport(ERROR,(errmsg("TEST_ONLY probe private mapping failed")));
 errno=0;
 if(strcmp(name,"move_pages")==0)result=syscall(SYS_move_pages,0,0,NULL,NULL,NULL,0);
 else if(strcmp(name,"migrate_pages")==0)result=syscall(SYS_migrate_pages,0,0,NULL,NULL);
 else if(strcmp(name,"mbind")==0)result=syscall(SYS_mbind,area,4096,0,NULL,0,0);
 else if(strcmp(name,"set_mempolicy")==0)result=syscall(SYS_set_mempolicy,0,NULL,0);
 else if(strcmp(name,"home_node")==0)result=syscall(SYS_set_mempolicy_home_node,area,4096,0,0);
 else if(strcmp(name,"io_uring")==0){struct io_uring_params p={0};result=syscall(SYS_io_uring_setup,1,&p);if(result>=0)close((int)result);}
 else if(strcmp(name,"userfaultfd")==0){result=syscall(SYS_userfaultfd,O_CLOEXEC);if(result>=0)close((int)result);}
 else if(strcmp(name,"process_madvise")==0)result=syscall(SYS_process_madvise,-1,NULL,0,MADV_DONTNEED,0);
 else if(strcmp(name,"process_mrelease")==0)result=syscall(SYS_process_mrelease,-1,0);
 else if(strcmp(name,"mlock")==0)result=syscall(SYS_mlock,area,4096);
 else if(strcmp(name,"madv_collapse")==0)result=syscall(SYS_madvise,area,4096,25);
 else if(strcmp(name,"madv_hugepage")==0)result=syscall(SYS_madvise,area,4096,MADV_HUGEPAGE);
 else if(strcmp(name,"madv_cold")==0)result=syscall(SYS_madvise,area,4096,20);
 else if(strcmp(name,"madv_pageout")==0)result=syscall(SYS_madvise,area,4096,21);
 else if(strcmp(name,"madv_mergeable")==0)result=syscall(SYS_madvise,area,4096,MADV_MERGEABLE);
 else if(strcmp(name,"dumpable_enable")==0)result=prctl(PR_SET_DUMPABLE,1,0,0,0);
 else if(strcmp(name,"vmsplice")==0){struct iovec io={area,4096};result=syscall(SYS_vmsplice,-1,&io,1,0);}
 else if(strcmp(name,"splice")==0)result=syscall(SYS_splice,-1,NULL,-1,NULL,0,0);
 else if(strcmp(name,"tee")==0)result=syscall(SYS_tee,-1,-1,0,0);
 else if(strcmp(name,"sendfile")==0)result=syscall(SYS_sendfile,-1,-1,NULL,0);
 else if(strcmp(name,"unshare")==0)result=syscall(SYS_unshare,0);
 else if(strcmp(name,"setns")==0)result=syscall(SYS_setns,-1,0);
 else if(strcmp(name,"ptrace")==0)result=syscall(SYS_ptrace,PTRACE_TRACEME,0,0,0);
 else if(strcmp(name,"pidfd_getfd")==0)result=syscall(SYS_pidfd_getfd,-1,0,0);
 else if(strcmp(name,"memfd_create")==0){result=syscall(SYS_memfd_create,"TEST_ONLY",0);if(result>=0)close((int)result);}
 else if(strcmp(name,"execve")==0){char *a[]={"/neonvm/bin/resource-guard","--TEST_ONLY-unexpected-exec",NULL};result=syscall(SYS_execve,a[0],a,NULL);}
 else if(strcmp(name,"fork")==0){result=syscall(SYS_fork);if(result==0)_exit(75);if(result>0){pid_t waited;do{waited=waitpid((pid_t)result,NULL,0);}while(waited<0&&errno==EINTR);}}
 else if(strcmp(name,"cgroup_write_open")==0){result=open("/sys/fs/cgroup/neon-postgres/memory.max",O_WRONLY|O_CLOEXEC);if(result>=0)close((int)result);}
 else if(strcmp(name,"raw_device_open")==0){result=open("/dev/vdc",O_RDWR|O_CLOEXEC);if(result>=0)close((int)result);}
 else if(strcmp(name,"mount")==0)result=mount("none","/tmp","tmpfs",0,NULL);
 else if(strcmp(name,"swapon")==0)result=swapon("/dev/vdc",0);
 else if(strcmp(name,"thp_disable_clear")==0)result=prctl(PR_SET_THP_DISABLE,0,0,0,0);
 else if(strcmp(name,"huge_reservation")==0){void *p=mmap(NULL,2<<20,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_NORESERVE,-1,0);result=p==MAP_FAILED?-1:0;if(p!=MAP_FAILED)munmap(p,2<<20);}
 else{munmap(area,4096);ereport(ERROR,(errmsg("unknown TEST_ONLY probe")));}
 saved=errno;munmap(area,4096);
 snprintf(out,sizeof(out),"{\"probe\":\"%s\",\"result\":%ld,\"errno\":%d,\"uid\":%u,\"euid\":%u,\"nnp\":%d,\"seccomp\":%d}",name,result,saved,(unsigned)getuid(),(unsigned)geteuid(),prctl(PR_GET_NO_NEW_PRIVS,0,0,0,0),prctl(PR_GET_SECCOMP,0,0,0,0));
 PG_RETURN_TEXT_P(cstring_to_text(out));
}
