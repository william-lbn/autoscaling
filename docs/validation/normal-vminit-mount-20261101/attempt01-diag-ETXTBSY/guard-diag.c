#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <sys/sysmacros.h>
#include <linux/magic.h>
#include <limits.h>
static dev_t rootdev;
static void fail(const char *s){fprintf(stderr,"DEV_DIAG_FAIL=%s errno=%d\n",s,errno);exit(101);}
static void must(int x,const char*s){if(!x)fail(s);}
static void syncdir(const char *p){int fd=open(p,O_RDONLY|O_DIRECTORY|O_CLOEXEC);must(fd>=0,p);must(fsync(fd)==0,"dir fsync");must(close(fd)==0,"dir close");}
static void put(const char*p,const char*s){int fd=open(p,O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0644);must(fd>=0,p);size_t n=strlen(s);must(write(fd,s,n)==(ssize_t)n,"write");must(fsync(fd)==0,"file fsync");must(close(fd)==0,"file close");}
static int isrootdev(const char*p){struct stat st;must(stat(p,&st)==0,p);return st.st_dev==rootdev;}
static void readonly_chain(const char*p){char real[PATH_MAX],at[PATH_MAX];must(realpath(p,real)!=NULL,"realpath");for(int pass=0;pass<2;pass++){snprintf(at,sizeof(at),"%s",pass?real:p);char*slash=strrchr(at,'/');must(slash!=NULL,"absolute");if(slash==at)at[1]=0;else*slash=0;for(;;){struct statfs fs;must(statfs(at,&fs)==0,"statfs ancestor");must((fs.f_flags&ST_RDONLY)!=0,"ancestor not RO");if(!strcmp(at,"/"))break;slash=strrchr(at,'/');if(slash==at)at[1]=0;else*slash=0;}}
 int fd=open(p,O_WRONLY|O_APPEND|O_CLOEXEC);must(fd<0&&errno==EROFS,"static writable or DAC-only rejection");printf("DEV_STATIC_RO=%s\n",p);}
static void dumpfile(const char*p,const char*prefix){FILE*f=fopen(p,"r");must(f!=NULL,p);char *s=NULL;size_t n=0;ssize_t k;size_t total=0;while((k=getline(&s,&n,f))>=0){total+=(size_t)k;must(total<=1<<20,"dump bound");must(!strstr(s,"root-writable"),"old RW view in mountinfo");printf("%s%s",prefix,s);}free(s);must(fclose(f)==0,"close dump");}
static void users(void){DIR*d=opendir("/proc");must(d!=NULL,"proc");struct dirent*de;int count=0;while((de=readdir(d))){if(de->d_name[0]<'0'||de->d_name[0]>'9')continue;int pid=atoi(de->d_name);if(pid<=0)continue;char path[256],target[PATH_MAX];snprintf(path,sizeof(path),"/proc/%d/exe",pid);ssize_t n=readlink(path,target,sizeof(target)-1);if(n<0){must(errno==ENOENT,"unknown proc exe");continue;}target[n]=0;count++;must(pid==1||pid==getpid(),"unexpected userspace child");printf("DEV_USER_PID=%d exe=%s\n",pid,target);
  snprintf(path,sizeof(path),"/proc/%d/task",pid);DIR*t=opendir(path);must(t!=NULL,"task dir");int tids=0;struct dirent*te;while((te=readdir(t)))if(te->d_name[0]>='0'&&te->d_name[0]<='9')tids++;closedir(t);must(tids==1,"unexpected user threads");
  for(int j=0;j<2;j++){snprintf(path,sizeof(path),"/proc/%d/%s",pid,j?"cwd":"root");struct stat st;struct statfs fs;must(stat(path,&st)==0&&statfs(path,&fs)==0,"root/cwd probe");must(st.st_dev!=rootdev||(fs.f_flags&ST_RDONLY),"RW root/cwd retained");}
  snprintf(path,sizeof(path),"/proc/%d/fd",pid);DIR*f=opendir(path);must(f!=NULL,"fd dir");struct dirent*fe;while((fe=readdir(f))){if(fe->d_name[0]<'0'||fe->d_name[0]>'9')continue;int fdnum=atoi(fe->d_name);char fp[256],info[256];snprintf(fp,sizeof(fp),"/proc/%d/fd/%d",pid,fdnum);snprintf(info,sizeof(info),"/proc/%d/fdinfo/%d",pid,fdnum);struct stat st;struct statfs fs;must(stat(fp,&st)==0,"fd stat");must(statfs(fp,&fs)==0,"fd statfs");ssize_t z=readlink(fp,target,sizeof(target)-1);must(z>=0,"fd link");target[z]=0;must(!strstr(target,"root-writable"),"RW alias fd retained");FILE*fi=fopen(info,"r");must(fi!=NULL,"fdinfo");char line[256];unsigned long flags=0;int seen=0;while(fgets(line,sizeof(line),fi))if(sscanf(line,"flags: %lo",&flags)==1)seen=1;fclose(fi);must(seen,"fd flags");printf("DEV_FD pid=%d fd=%d flags=%lo target=%s\n",pid,fdnum,flags,target);if(st.st_dev==rootdev){must(fs.f_flags&ST_RDONLY,"nonRO root-filesystem fd retained");must((flags&O_ACCMODE)==O_RDONLY,"root regular writable fd retained");}if(S_ISBLK(st.st_mode)&&st.st_rdev==rootdev)must((flags&O_ACCMODE)==O_RDONLY,"RW raw root block fd retained");}closedir(f);
  snprintf(path,sizeof(path),"/proc/%d/maps",pid);FILE*m=fopen(path,"r");must(m!=NULL,"maps");char*line=NULL;size_t cap=0;size_t total=0;while((n=getline(&line,&cap,m))>=0){total+=(size_t)n;must(total<=1<<20,"maps bound");unsigned long lo,hi,off;char perm[5];unsigned ma,mi;unsigned long long ino;if(sscanf(line,"%lx-%lx %4s %lx %x:%x %llu",&lo,&hi,perm,&off,&ma,&mi,&ino)==7){must(!(ma==major(rootdev)&&mi==minor(rootdev)&&perm[1]=='w'&&perm[3]=='s'),"shared writable ext4 map retained");}}free(line);fclose(m);
 }closedir(d);must(count==2,"exact PID1 and diagnostic expected");printf("DEV_NO_RESIDUAL_RW_FD_MAP_CHILD=1 users=%d\n",count);}
int main(int argc,char**argv){setvbuf(stdout,NULL,_IOLBF,0);if(argc!=2)return 104;if(!strcmp(argv[1],"check-guest")){fprintf(stderr,"DEV_CHECK_GUEST_REFUSED=43\n");return 43;}if(strcmp(argv[1],"initial-boot"))return 105;
 printf("DEV_INITIAL_BOOT_DIAGNOSTIC_ONLY=1\n");struct stat st;struct statfs fs;must(stat("/",&st)==0,"root stat");rootdev=st.st_dev;must(statfs("/",&fs)==0&&fs.f_type==EXT4_SUPER_MAGIC&&(fs.f_flags&ST_RDONLY),"RO ext4 root");
 const char*statics[]={"/neonvm/bin/vminit","/neonvm/bin/dsga-resource-service","/neonvm/bin/udev-init.sh","/neonvm/bin/vmshutdown","/neonvm/bin/busybox","/neonvm/bin/sh","/neonvm/bin/dsga-resource-guard","/lib/ld-musl-x86_64.so.1","/usr/local/bin/compute_ctl","/usr/local/bin/postgres","/usr/local/lib/fixture-core.so","/etc/inittab"};for(size_t i=0;i<sizeof(statics)/sizeof(*statics);i++)readonly_chain(statics[i]);
 const char*holes[]={"/etc/neon-dsga","/var/lib/neon-dsga","/var/db/postgres","/var/log","/var/lib/chrony","/etc/resolv.conf"};for(size_t i=0;i<sizeof(holes)/sizeof(*holes);i++){must(isrootdev(holes[i]),"persistent hole changed filesystem");must(statfs(holes[i],&fs)==0&&fs.f_type==EXT4_SUPER_MAGIC&&!(fs.f_flags&ST_RDONLY),"hole not RW ext4");printf("DEV_PERSISTENT_EXT4=%s\n",holes[i]);}
 must(lstat("/run/neon-dsga/root-writable",&st)<0&&errno==ENOENT,"temporary RW root view still exists");dumpfile("/proc/self/mountinfo","DEV_MOUNTINFO_SELF ");dumpfile("/proc/1/mountinfo","DEV_MOUNTINFO_PID1 ");
 must(mkdir("/var/lib/neon-dsga/state",0700)==0,"create state");put("/var/lib/neon-dsga/state/fixture-state.bin","ROOT_STATE_PERSISTENT_V1\n");syncdir("/var/lib/neon-dsga/state");syncdir("/var/lib/neon-dsga");put("/etc/neon-dsga/fixture-config.bin","ROOT_CONFIG_PERSISTENT_V1\n");syncdir("/etc/neon-dsga");
 pid_t c=fork();must(c>=0,"data fork");if(c==0){must(setgid(1000)==0&&setuid(1000)==0,"drop data identity");must(mkdir("/var/db/postgres/compute",0700)==0,"mkdir PGDATA");put("/var/db/postgres/compute/first","first\n");must(unlink("/var/db/postgres/compute/first")==0&&rmdir("/var/db/postgres/compute")==0,"remove PGDATA");must(mkdir("/var/db/postgres/compute",0700)==0,"recreate PGDATA");put("/var/db/postgres/compute/fixture.pg","PGDATA_PERSISTENT_UID1000_V1\n");syncdir("/var/db/postgres/compute");must(mkdir("/var/db/postgres/compute.warmup",0700)==0,"warmup sibling");put("/var/db/postgres/compute.warmup/fixture.warm","WARMUP_PERSISTENT_UID1000_V1\n");syncdir("/var/db/postgres/compute.warmup");syncdir("/var/db/postgres");_exit(0);}int status;must(waitpid(c,&status,0)==c&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"data child wait");printf("DEV_PGDATA_PARENT_RECREATE_WARMUP_UID1000=1\n");
 const char*temp[]={"/run","/tmp","/dev/shm"};const char*paths[]={"/run/neon-dsga/fixture-run","/tmp/fixture-tmp","/dev/shm/fixture-shm"};for(int i=0;i<3;i++){must(statfs(temp[i],&fs)==0&&fs.f_type==TMPFS_MAGIC&&!(fs.f_flags&ST_RDONLY),"temporary hole not TMPFS");put(paths[i],"EPHEMERAL_NOT_EXT4\n");printf("DEV_TMPFS=%s\n",temp[i]);}
 users();printf("DEV_MOUNT_DIAG_PASS=1\n");printf("DEV_FORCED_EXIT=42\n");return 42;}
