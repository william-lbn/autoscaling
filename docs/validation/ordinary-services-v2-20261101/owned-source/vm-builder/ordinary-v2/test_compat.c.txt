/* TEST_ONLY component driver; raw original children, not production Gate/Source. */
#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/sysmacros.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <sys/prctl.h>
#include <sys/un.h>
#include <linux/capability.h>
#include <signal.h>
#include <grp.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#ifndef SO_PEERPIDFD
#define SO_PEERPIDFD 77
#endif
static void fail(const char *s){fprintf(stderr,"COMPAT_FAIL %s errno=%d\n",s,errno);exit(1);}
static void drop(unsigned id){struct __user_cap_header_struct h={_LINUX_CAPABILITY_VERSION_3,0};struct __user_cap_data_struct c[2]={{0},{0}};if(setgroups(0,0)||setresgid(id,id,id)||setresuid(id,id,id)||syscall(SYS_capset,&h,c)||prctl(PR_SET_NO_NEW_PRIVS,1,0,0,0))_exit(91);}
struct child{pid_t pid;int pidfd;};
static struct child spawn(unsigned id,char *const argv[]){pid_t p=fork();if(p<0)fail("fork");if(!p){drop(id);execv(argv[0],argv);_exit(92);}int fd=syscall(SYS_pidfd_open,p,0);if(fd<0)fail("original child pidfd");return(struct child){p,fd};}
static int reap(struct child c){int st;if(waitpid(c.pid,&st,0)!=c.pid)fail("original child wait");close(c.pidfd);return WIFEXITED(st)?WEXITSTATUS(st):128+WTERMSIG(st);}
static void stop(struct child c){if(syscall(SYS_pidfd_send_signal,c.pidfd,SIGTERM,0,0))fail("stop original child");int code=reap(c);if(code!=0)fail("actual child normal exit");}
static int live(struct child c){struct pollfd p={c.pidfd,POLLIN,0};return poll(&p,1,0)==0;}
static void require_uid(struct child c,unsigned id){char path[64],buf[16384];snprintf(path,sizeof path,"/proc/%d/status",c.pid);int fd=open(path,O_RDONLY|O_CLOEXEC);if(fd<0)fail("status");ssize_t n=read(fd,buf,sizeof buf-1);close(fd);if(n<=0)fail("status bytes");buf[n]=0;char needle[128];snprintf(needle,sizeof needle,"Uid:\t%u\t%u\t%u\t%u",id,id,id,id);if(!strstr(buf,needle)||!strstr(buf,"NoNewPrivs:\t1")||!strstr(buf,"CapEff:\t0000000000000000"))fail("actual nonRoot NNP cap0");}
static void root_command(char *const a[]){pid_t p=fork();if(p<0)fail("root helper fork");if(!p){execv(a[0],a);_exit(93);}int st;if(waitpid(p,&st,0)!=p||!WIFEXITED(st)||WEXITSTATUS(st))fail("real udevadm command");}
static void negative(void){char *u[]={"/sbin/udevd","--neon-fixed-devices-v1",0};struct child c=spawn(60005,u);int code=reap(c);if(code==0)fail("wrong udev UID accepted");char *a[]={"/sbin/acpid","-f","-n","--neon-fixed-power-v1",0};c=spawn(60005,a);code=reap(c);if(code==0)fail("wrong ACPI UID accepted");puts("ORDINARY_NEGATIVE_WRONG_UID_PASS");}
static void test_udev(void){char *a[]={"/sbin/udevd","--neon-fixed-devices-v1",0};struct child c=spawn(60004,a);for(int i=0;i<100;i++){struct stat st;if(!lstat("/run/udev/control",&st)&&S_ISSOCK(st.st_mode))break;if(!live(c))fail("udevd startup");usleep(25000);}if(!live(c))fail("udevd exited");require_uid(c,60004);char *ping[]={"/bin/udevadm","control","--start-exec-queue","--timeout=5",0};root_command(ping);char *trigger[]={"/bin/udevadm","trigger","--action=add","--subsystem-match=virtio-ports",0};root_command(trigger);char *settle[]={"/bin/udevadm","settle","--timeout=5",0};root_command(settle);struct stat st;if(stat("/dev/virtio-ports/tech.neon.log.0",&st)||!S_ISCHR(st.st_mode)||st.st_uid!=0||st.st_gid!=60007)fail("original Root log device changed");char dbpath[128],db[16384];snprintf(dbpath,sizeof dbpath,"/run/udev/data/c%u:%u",major(st.st_rdev),minor(st.st_rdev));int dbfd=open(dbpath,O_RDONLY|O_CLOEXEC);if(dbfd<0)fail("actual worker event database missing");ssize_t size=read(dbfd,db,sizeof db-1);close(dbfd);if(size<=0)fail("worker database empty");db[size]=0;if(!strstr(db,"E:NEON_FIXED_TEST=executed"))fail("actual coldplug worker did not execute fixed rule");require_uid(c,60004);if(!live(c))fail("udevd worker failed after coldplug");puts("ORDINARY_EUDEV_NONROOT_CONTROL_COLDPLUG_PASS");stop(c);}
static void test_acpi(void){int s=socket(AF_UNIX,SOCK_SEQPACKET|SOCK_CLOEXEC,0);if(s<0)fail("Root fixed socket");int one=1;if(setsockopt(s,SOL_SOCKET,SO_PASSCRED,&one,sizeof one))fail("Root PASSCRED");struct sockaddr_un a={.sun_family=AF_UNIX};strcpy(a.sun_path,"/run/neon-dsga/ordinary-acpi.sock");if(bind(s,(struct sockaddr*)&a,sizeof a)||chown(a.sun_path,0,60006)||chmod(a.sun_path,0660)||listen(s,2))fail("Root fixed listener");char *cmd[]={"/sbin/acpid","-f","-n","-c","/neonvm/acpi","-s","/run/neon-ordinary/acpi/acpid.socket","--neon-fixed-power-v1",0};struct child c=spawn(60006,cmd);usleep(250000);if(!live(c))fail("acpid startup");require_uid(c,60006);puts("ORDINARY_ACPI_READY");fflush(stdout);struct pollfd poller={s,POLLIN,0};if(poll(&poller,1,20000)!=1)fail("actual kernel powerbutton event not received");int fd=accept4(s,0,0,SOCK_CLOEXEC);if(fd<0)fail("Root accepted original daemon");struct ucred peer;socklen_t len=sizeof peer;if(getsockopt(fd,SOL_SOCKET,SO_PEERCRED,&peer,&len)||peer.pid!=c.pid||peer.uid!=60006||peer.gid!=60006)fail("original daemon peer");int peerfd=-1;len=sizeof peerfd;if(getsockopt(fd,SOL_SOCKET,SO_PEERPIDFD,&peerfd,&len)||peerfd<0)fail("original peer pidfd");struct stat x,y;if(fstat(peerfd,&x)||fstat(c.pidfd,&y)||x.st_dev!=y.st_dev||x.st_ino!=y.st_ino)fail("original peer kernel object differs");close(peerfd);unsigned char raw[16];char control[CMSG_SPACE(sizeof(struct ucred))];struct iovec iov={raw,sizeof raw};struct msghdr msg={.msg_iov=&iov,.msg_iovlen=1,.msg_control=control,.msg_controllen=sizeof control};ssize_t n=recvmsg(fd,&msg,MSG_TRUNC);static const unsigned char want[16]={'N','D','S','G','O','A','0','2',0,0,0,2,0,0,0,1};if(n!=16||msg.msg_flags&(MSG_TRUNC|MSG_CTRUNC)||memcmp(raw,want,16))fail("fixed power request");struct cmsghdr *h=CMSG_FIRSTHDR(&msg);if(!h||h->cmsg_level!=SOL_SOCKET||h->cmsg_type!=SCM_CREDENTIALS||h->cmsg_len!=CMSG_LEN(sizeof(struct ucred))||CMSG_NXTHDR(&msg,h))fail("exact one original per-packet credential");struct ucred actual;memcpy(&actual,CMSG_DATA(h),sizeof actual);if(actual.pid!=c.pid||actual.uid!=60006||actual.gid!=60006||!live(c))fail("forked child or wrong credential");static const unsigned char ack[16]={'N','D','S','G','O','A','O','K',0,0,0,2,0,0,0,1};if(send(fd,ack,16,MSG_NOSIGNAL)!=16)fail("Root fixed ACK");close(fd);struct pollfd duplicate={s,POLLIN,0};if(poll(&duplicate,1,1200)!=0)fail("same power event caused more than one fixed request");close(s);if(!live(c))fail("acpid did not return to event loop");require_uid(c,60006);puts("ORDINARY_ACPI_REQUEST_COUNT=1");puts("ORDINARY_ACPI_SAME_DAEMON_PIDFD_PACKET_PASS");stop(c);}
int main(void){setbuf(stdout,0);negative();test_udev();test_acpi();puts("ORDINARY_COMPAT_PASS_NOT_GATE_OR_NORMAL");return 0;}
