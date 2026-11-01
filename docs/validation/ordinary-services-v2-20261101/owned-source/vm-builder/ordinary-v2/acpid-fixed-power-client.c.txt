/* Neon ordinary v2 fixed-power client. Not an authorization producer. */
#include <errno.h>
#include <fcntl.h>
#include <linux/capability.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
int neon_fixed_power_mode;
int neon_fixed_power_preflight(void) {
 uid_t r,e,s;gid_t gr,ge,gs;
 struct __user_cap_header_struct h={_LINUX_CAPABILITY_VERSION_3,0};
 struct __user_cap_data_struct c[2]={{0},{0}};
 if(getresuid(&r,&e,&s)||getresgid(&gr,&ge,&gs)||r!=60006||e!=60006||s!=60006||gr!=60006||ge!=60006||gs!=60006||getgroups(0,0)!=0||prctl(PR_GET_NO_NEW_PRIVS,0,0,0,0)!=1||syscall(SYS_capget,&h,c)||c[0].effective||c[0].permitted||c[0].inheritable||c[1].effective||c[1].permitted||c[1].inheritable)return -1;
 return 0;
}
static int64_t neon_now_ms(void){struct timespec t;if(clock_gettime(CLOCK_MONOTONIC,&t))return -1;return(int64_t)t.tv_sec*1000+t.tv_nsec/1000000;}
static int neon_poll(int fd,short events,int64_t deadline){for(;;){int64_t now=neon_now_ms();if(now<0||now>=deadline)return -1;struct pollfd p={fd,events,0};int rc=poll(&p,1,(int)(deadline-now));if(rc<0&&errno==EINTR)continue;if(rc!=1||p.revents&(POLLERR|POLLNVAL)||!(p.revents&events)||(events==POLLOUT&&(p.revents&POLLHUP)))return -1;return 0;}}
static int neon_fixed_power_request(void){
 /* 同一按钮可能同时从netlink/input到达。仅完整原Root ACK后记本进程确认，
  * 后续相同固定动作不再发送；丢ACK绝不记成功/重试。Root仍须独立持久
  * 原VMboot/role lease的at-most-once，不能以此进程缓存冒充Root资格。 */
 static int confirmed;
 if(confirmed)return 0;
 static const unsigned char request[16]={'N','D','S','G','O','A','0','2',0,0,0,2,0,0,0,1};
 static const unsigned char expected[16]={'N','D','S','G','O','A','O','K',0,0,0,2,0,0,0,1};
 struct sockaddr_un a={.sun_family=AF_UNIX};struct stat st;int fd=-1,result=-1,stage=1;int64_t start=neon_now_ms();
 if(start<0||neon_fixed_power_preflight())return -1;
 if(lstat("/run/neon-dsga",&st)||!S_ISDIR(st.st_mode)||st.st_uid!=0||st.st_gid!=0||(st.st_mode&07777)!=0755)return -1;
 if(lstat("/run/neon-dsga/ordinary-acpi.sock",&st)||!S_ISSOCK(st.st_mode)||st.st_uid!=0||st.st_gid!=60006||(st.st_mode&07777)!=0660)return -1;
 strcpy(a.sun_path,"/run/neon-dsga/ordinary-acpi.sock");
 fd=socket(AF_UNIX,SOCK_SEQPACKET|SOCK_CLOEXEC|SOCK_NONBLOCK,0);if(fd<0)return -1;
 stage=2; if(connect(fd,(struct sockaddr*)&a,sizeof(a)))goto done;
 stage=3; struct ucred peer;socklen_t len=sizeof(peer);
 if(getsockopt(fd,SOL_SOCKET,SO_PEERCRED,&peer,&len)||len!=sizeof(peer)||peer.uid!=0||peer.gid!=0||peer.pid<=1)goto done;
 stage=4; if(neon_poll(fd,POLLOUT,start+2000))goto done;
 stage=5; if(send(fd,request,sizeof(request),MSG_NOSIGNAL)!=(ssize_t)sizeof(request))goto done;
 stage=6; if(neon_poll(fd,POLLIN,start+2000))goto done;
 unsigned char reply[16];struct iovec iov={reply,sizeof(reply)};struct msghdr msg={.msg_iov=&iov,.msg_iovlen=1};
 stage=7; if(recvmsg(fd,&msg,MSG_TRUNC)!=(ssize_t)sizeof(reply)||(msg.msg_flags&(MSG_TRUNC|MSG_CTRUNC))||memcmp(reply,expected,sizeof(reply)))goto done;
 result=0;confirmed=1;
 done:if(result){int saved=errno;fprintf(stderr,"fixed-power exchange remains Unknown stage=%d errno=%d\n",stage,saved);}close(fd);return result;
}
