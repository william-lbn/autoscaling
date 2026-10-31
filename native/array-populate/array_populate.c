/* PG版本中性、固定FD的短命数组预触者。没有libc/heap/线程/网络；
 * 由Root以CLONE_INTO_CGROUP原子出生arrays，GO前锁定自身全部PT_LOAD页(含64K栈)。
 * A预算为自身保守预留；既有ELF cache仍按真实System/K收费，不能称迁费。
 * 取消不能承诺madvise前缀无收费；原OFD与父pidfd终态/WAL负责拒绝迟到资源写者。 */
#include <stdint.h>
#include <stddef.h>
#include <elf.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <linux/audit.h>
#include <linux/prctl.h>
#include <linux/capability.h>
#include <asm/unistd.h>

#define PLAN_BYTES 368
#define STATUS_BYTES 256
#define CTRL 9
#define MSG_TRUNC 0x20
#define PARENTFD 10
#define PAGE 4096ULL
#define PERM (SECCOMP_RET_ERRNO|1U)
#define TMPFS_MAGIC 0x01021994
#define SIGKILL 9
#define POLLIN 1
#define POLLHUP 16
#define POLLERR 8
#define LIMIT_AS 9
#define LIMIT_MEMLOCK 8
#define LIMIT_NOFILE 7
#define ARRAY_ADVICE 23 /* Linux6.12 MADV_POPULATE_WRITE */

__attribute__((used,aligned(4096))) unsigned char helper_stack[65536];
extern unsigned char __executable_start[];
static unsigned char plan[PLAN_BYTES],status[STATUS_BYTES],command;
static struct sock_filter program[128];
static unsigned int pc;
static uint64_t bases[4],lengths[4],locked_bytes;
static volatile unsigned char sink;
struct helper_pollfd {int fd;short events,revents;};
static struct helper_pollfd parent_poll={PARENTFD,POLLIN,0};
static long sc(long n,long a,long b,long c,long d,long e,long f){
 register long r10 __asm__("r10")=d;register long r8 __asm__("r8")=e;register long r9 __asm__("r9")=f;long ret;
 __asm__ volatile("syscall":"=a"(ret):"a"(n),"D"(a),"S"(b),"d"(c),"r"(r10),"r"(r8),"r"(r9):"rcx","r11","memory");return ret;
}
static uint64_t u64(const unsigned char *p){uint64_t x=0;for(int i=7;i>=0;i--)x=(x<<8)|p[i];return x;}
static uint32_t u32(const unsigned char *p){return(uint32_t)u64((const unsigned char[8]){p[0],p[1],p[2],p[3],0,0,0,0});}
static void w32(unsigned char*p,uint32_t x){for(int i=0;i<4;i++){p[i]=(unsigned char)x;x>>=8;}}
static void w64(unsigned char*p,uint64_t x){for(int i=0;i<8;i++){p[i]=(unsigned char)x;x>>=8;}}
static void copy(unsigned char*d,const unsigned char*s,size_t n){for(size_t i=0;i<n;i++)d[i]=s[i];}
static int equal(const unsigned char*a,const unsigned char*b,size_t n){unsigned int x=0;for(size_t i=0;i<n;i++)x|=a[i]^b[i];return x==0;}
__attribute__((noreturn)) static void leave(int code){sc(__NR_exit_group,code,0,0,0,0,0);for(;;){}}
/* getpid在filter后也不能用；PID在GO前缓存。 */
static uint64_t own_pid;
static void report_ready(uint32_t kind,int error,uint32_t index){
 for(int i=0;i<STATUS_BYTES;i++)status[i]=0;
 copy(status,(const unsigned char*)"NAPS0001",8);w32(status+8,kind);w32(status+12,(uint32_t)error);w64(status+16,locked_bytes);w64(status+24,own_pid);
 for(int i=0;i<4;i++){w64(status+32+16*i,bases[i]);w64(status+40+16*i,lengths[i]);}
 copy(status+96,plan+144,32);w32(status+128,pc);w32(status+132,index);
 if(sc(__NR_write,CTRL,(long)status,STATUS_BYTES,0,0,0)!=STATUS_BYTES)leave(112);
}
__attribute__((noreturn)) static void fail(long error){report_ready(3,(int)(error<0?-error:error),0);leave(111);}
static int parent_alive(void){parent_poll.revents=0;long r=sc(__NR_poll,(long)&parent_poll,1,0,0,0,0);return r==0&&parent_poll.revents==0;}
static void emit(uint16_t code,uint8_t jt,uint8_t jf,uint32_t k){if(pc>=128)leave(113);program[pc++]=(struct sock_filter){code,jt,jf,k};}
static void rule(long nr,const uint32_t *offset,const uint32_t *value,unsigned int n){
 emit(BPF_LD|BPF_W|BPF_ABS,0,0,0);emit(BPF_JMP|BPF_JEQ|BPF_K,0,(uint8_t)(2*n+1),(uint32_t)nr);
 for(unsigned int i=0;i<n;i++){emit(BPF_LD|BPF_W|BPF_ABS,0,0,offset[i]);emit(BPF_JMP|BPF_JEQ|BPF_K,0,(uint8_t)(2*(n-i-1)+1),value[i]);}
 emit(BPF_RET|BPF_K,0,0,SECCOMP_RET_ALLOW);
}
static void install_filter(void){
 emit(BPF_LD|BPF_W|BPF_ABS,0,0,4);emit(BPF_JMP|BPF_JEQ|BPF_K,1,0,AUDIT_ARCH_X86_64);emit(BPF_RET|BPF_K,0,0,SECCOMP_RET_KILL_PROCESS);
 emit(BPF_LD|BPF_W|BPF_ABS,0,0,0);emit(BPF_JMP|BPF_JSET|BPF_K,0,1,0x40000000U);emit(BPF_RET|BPF_K,0,0,SECCOMP_RET_KILL_PROCESS);
 const uint32_t fdcount[]={16,20,32,36};uint32_t writev[]={CTRL,0,STATUS_BYTES,0};
 const uint32_t roff[]={16,20,32,36,40,44,48,52,56,60};const uint32_t rval[]={CTRL,0,1,0,MSG_TRUNC,0,0,0,0,0};
 rule(__NR_recvfrom,roff,rval,10);rule(__NR_write,fdcount,writev,4);
 const uint32_t pollargs[]={24,28,32,36};const uint32_t pollv[]={1,0,0,0};rule(__NR_poll,pollargs,pollv,4);
 rule(__NR_exit_group,0,0,0);rule(__NR_exit,0,0,0);
 const uint32_t maoff[]={16,20,24,28,32,36};
 for(int i=0;i<4;i++)if(lengths[i]){uint32_t v[]={(uint32_t)bases[i],(uint32_t)(bases[i]>>32),(uint32_t)lengths[i],(uint32_t)(lengths[i]>>32),ARRAY_ADVICE,0};rule(__NR_madvise,maoff,v,6);}
 emit(BPF_RET|BPF_K,0,0,PERM);struct sock_fprog fp={(unsigned short)pc,program};long r=sc(__NR_prctl,PR_SET_SECCOMP,SECCOMP_MODE_FILTER,(long)&fp,0,0,0);if(r)fail(r);
}
static void set_limit(long which,uint64_t value){struct rlimit l={(rlim_t)value,(rlim_t)value};long r=sc(__NR_setrlimit,which,(long)&l,0,0,0,0);if(r)fail(r);}
static void lock_image(uint64_t bound){
 Elf64_Ehdr *e=(Elf64_Ehdr*) __executable_start;
 if(!equal(e->e_ident,(const unsigned char*)"\177ELF",4)||e->e_ident[EI_CLASS]!=ELFCLASS64||e->e_machine!=EM_X86_64||e->e_type!=ET_EXEC||e->e_phnum>16||e->e_phentsize!=sizeof(Elf64_Phdr))fail(22);
 Elf64_Phdr*p=(Elf64_Phdr*)(__executable_start+e->e_phoff);
 for(unsigned int i=0;i<e->e_phnum;i++)if(p[i].p_type==PT_LOAD){
  uint64_t a=p[i].p_vaddr&~(PAGE-1),end=p[i].p_vaddr+p[i].p_memsz;
  if(end<p[i].p_vaddr||end>UINT64_MAX-(PAGE-1))fail(22);
  end=(end+PAGE-1)&~(PAGE-1);
  if(end<a||end-a>bound-locked_bytes)fail(12);
  long r=sc(__NR_mlock,(long)a,(long)(end-a),0,0,0,0);if(r)fail(r);
  for(uint64_t q=a;q<end;q+=PAGE){volatile unsigned char*x=(volatile unsigned char*)q;unsigned char v=*x;if(p[i].p_flags&PF_W)*x=v;else sink^=v;}
  locked_bytes+=end-a;
 }
 if(locked_bytes==0||locked_bytes>bound)fail(12);
}
__attribute__((used,noreturn)) void helper_main(void){
 own_pid=(uint64_t)sc(__NR_getpid,0,0,0,0,0,0);
 if(sc(__NR_prctl,PR_SET_PDEATHSIG,SIGKILL,0,0,0,0))leave(110);
 if(sc(__NR_recvfrom,CTRL,(long)plan,PLAN_BYTES,MSG_TRUNC,0,0)!=PLAN_BYTES)leave(110);
 if(!equal(plan,(const unsigned char*)"NAPOP001",8)||u32(plan+8)!=1||u32(plan+12)!=0||u64(plan+16)<2||sc(__NR_getppid,0,0,0,0,0,0)!=(long)u64(plan+16)||!parent_alive()||u32(plan+24)!=65534||u32(plan+28)!=65534)fail(22);
 for(int i=96;i<112;i++)if(plan[i])fail(22);
 uint64_t lock_bound=u64(plan+32),as=u64(plan+40);if(lock_bound<PAGE||lock_bound>1048576||lock_bound%PAGE||as<1048576||as>INT64_MAX||u64(plan+128)==0||u64(plan+136)==0)fail(22);
 struct stat st;struct statfs fs;if(sc(__NR_fstat,4,(long)&st,0,0,0,0)||st.st_dev!=u64(plan+48)||st.st_ino!=u64(plan+56)||st.st_uid!=0||(st.st_mode&0077)||(st.st_mode&S_IFMT)!=S_IFREG)fail(22);
 /* AS必须先于任何目标mmap；setrlimit不证明现有VmSize已在界内，Root还须实际复核。 */
 set_limit(LIMIT_AS,as);set_limit(LIMIT_MEMLOCK,lock_bound);set_limit(LIMIT_NOFILE,11);
 for(int i=0;i<4;i++){
  const unsigned char*p=plan+208+40*i;uint64_t file=u64(p+16),off=u64(p+24),len=u64(p+32);
  if(!file||file%PAGE||off%PAGE||len%PAGE||off>file||len>file-off||len>INT64_MAX||sc(__NR_fstat,5+i,(long)&st,0,0,0,0)||sc(__NR_fstatfs,5+i,(long)&fs,0,0,0,0)||fs.f_type!=TMPFS_MAGIC||(st.st_mode&S_IFMT)!=S_IFREG||st.st_dev!=u64(p)||st.st_ino!=u64(p+8)||st.st_size!=(long)file)fail(22);
  if(len){long a=sc(__NR_mmap,0,(long)len,PROT_READ|PROT_WRITE,MAP_SHARED,5+i,(long)off);if(a<0&&a>=-4095)fail(a);bases[i]=(uint64_t)a;lengths[i]=len;}
 }
 if(sc(__NR_prctl,PR_SET_KEEPCAPS,0,0,0,0,0)||sc(__NR_setgroups,0,0,0,0,0,0)||sc(__NR_setresgid,65534,65534,65534,0,0,0)||sc(__NR_setresuid,65534,65534,65534,0,0,0))fail(1);
 struct __user_cap_header_struct ch={_LINUX_CAPABILITY_VERSION_3,0};struct __user_cap_data_struct cd[2]={{0,0,0},{0,0,0}};
 if(sc(__NR_capset,(long)&ch,(long)cd,0,0,0,0)||sc(__NR_prctl,PR_CAP_AMBIENT,PR_CAP_AMBIENT_CLEAR_ALL,0,0,0,0))fail(1);
 /* setuid会清PDEATHSIG；降权后重新设。mlock此时确受原hard memlock约束。 */
 if(sc(__NR_prctl,PR_SET_PDEATHSIG,SIGKILL,0,0,0,0)||sc(__NR_getppid,0,0,0,0,0,0)!=(long)u64(plan+16)||!parent_alive())fail(3);
 lock_image(lock_bound);
 if(sc(__NR_prctl,PR_SET_NO_NEW_PRIVS,1,0,0,0,0)||sc(__NR_prctl,PR_SET_DUMPABLE,0,0,0,0,0))fail(1);
 install_filter();report_ready(1,0,0);
 for(int i=0;i<4;i++)if(lengths[i]){
  if(sc(__NR_recvfrom,CTRL,(long)&command,1,MSG_TRUNC,0,0)!=1||command!=(unsigned char)(0xa0+i)||!parent_alive())leave(114);
  long x=sc(__NR_madvise,(long)bases[i],(long)lengths[i],ARRAY_ADVICE,0,0,0);if(x)fail(x);report_ready(2,0,(uint32_t)i);
 }
 if(sc(__NR_recvfrom,CTRL,(long)&command,1,MSG_TRUNC,0,0)!=1||command!=0xef||!parent_alive())leave(114);
 leave(0);
}
__asm__(".global _start\n_start:\nxor %ebp,%ebp\nlea helper_stack+65536(%rip),%rsp\nand $-16,%rsp\ncall helper_main\nud2\n");
