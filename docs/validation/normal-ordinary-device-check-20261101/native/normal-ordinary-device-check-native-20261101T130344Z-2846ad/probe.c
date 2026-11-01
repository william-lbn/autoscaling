#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <sys/statfs.h>
#include <linux/capability.h>
#include <unistd.h>
#include <grp.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <fcntl.h>
static const char *path="/run/neon-dsga/ordinary-udev-init.sock";
static const unsigned char request[16]={'N','D','S','G','O','A','0','2',0,0,0,2,0,0,0,5};
static const unsigned char ack[16]={'N','D','S','G','O','A','O','K',0,0,0,2,0,0,0,5};
static void check(int ok,const char *where){if(!ok){perror(where);exit(80);}}
static void drop(int mode){
 check(!prctl(PR_SET_NO_NEW_PRIVS,1,0,0,0),"nnp");
 for(int bit=0;bit<64;bit++){if(prctl(PR_CAPBSET_DROP,bit,0,0,0)&&errno!=EINVAL)check(0,"drop bound");}
 check(!setgroups(0,NULL),"groups");check(!setresgid(60005,60005,60005),"gid");
 if(mode==5)check(!setresuid(60004,60004,60004),"wronguid");
 else if(mode==6)check(!setresuid(60005,0,0),"suid identity");
 else check(!setresuid(60005,60005,60005),"uid");
 struct __user_cap_header_struct h={_LINUX_CAPABILITY_VERSION_3,0};struct __user_cap_data_struct d[2]={{0},{0}};
 check(!syscall(SYS_capset,&h,d),"cap0");
 char *const argv[]={"/neonvm/bin/dsga-resource-guard","ordinary-v2-device-check",NULL};char *const env[]={"PATH=/neonvm/bin",NULL};
 if(mode==6){uid_t a,b,c;getresuid(&a,&b,&c);fprintf(stderr,"DEVICE_CHECK_PREEXEC_UID %u %u %u\n",a,b,c);fflush(stderr);}
 execve(argv[0],argv,env);_exit(93);
}
static int wait_child(pid_t child){int status=0;check(waitpid(child,&status,0)==child,"unique wait");check(WIFEXITED(status),"child not normal exit");return WEXITSTATUS(status);}
static void testcase(int mode,const char *name){
 int listener=socket(AF_UNIX,SOCK_SEQPACKET|SOCK_CLOEXEC,0);check(listener>=0,"listener");
 struct sockaddr_un address={.sun_family=AF_UNIX};strcpy(address.sun_path,path);
 check(!bind(listener,(struct sockaddr*)&address,sizeof address),"bind");check(!chown(path,0,60005),"socket group");check(!chmod(path,mode==7?0666:0660),"socket mode");
 int on=1;check(!setsockopt(listener,SOL_SOCKET,SO_PASSCRED,&on,sizeof on),"passcred");check(!listen(listener,2),"listen");
 pid_t child=fork();check(child>=0,"client clone");if(!child)drop(mode);
 struct pollfd poller={listener,POLLIN,0};int connected=poll(&poller,1,mode>=5?500:6000);check(connected>=0,"accept poll");
 int packets=0;
 if(mode>=5){if(connected){char namebuf[96],line[512];snprintf(namebuf,sizeof namebuf,"/proc/%d/status",child);FILE *fp=fopen(namebuf,"r");if(fp){while(fgets(line,sizeof line,fp))if(!strncmp(line,"Uid:",4)||!strncmp(line,"Gid:",4)||!strncmp(line,"Cap",3)||!strncmp(line,"NoNewPrivs:",11))fprintf(stderr,"DEVICE_CHECK_CONNECTED_STATUS %s",line);fclose(fp);}fflush(stderr);}check(connected==0,"uninstalled caller connected");}
 else{
  check(connected==1&&poller.revents&POLLIN,"client not connected");int fd=accept4(listener,NULL,NULL,SOCK_CLOEXEC);check(fd>=0,"accept");
  unsigned char raw[17],control[256];struct iovec io={raw,sizeof raw};struct msghdr msg={.msg_iov=&io,.msg_iovlen=1,.msg_control=control,.msg_controllen=sizeof control};
  ssize_t size=recvmsg(fd,&msg,MSG_CMSG_CLOEXEC);check(size==16&&!(msg.msg_flags&(MSG_TRUNC|MSG_CTRUNC))&&!memcmp(raw,request,16),"exact action5");
  struct ucred *credential=NULL;int count=0;
  for(struct cmsghdr *c=CMSG_FIRSTHDR(&msg);c;c=CMSG_NXTHDR(&msg,c)){check(c->cmsg_level==SOL_SOCKET&&c->cmsg_type==SCM_CREDENTIALS,"only kernel credential");credential=(struct ucred*)CMSG_DATA(c);count++;}
  check(count==1&&credential&&credential->pid==child&&credential->uid==60005&&credential->gid==60005,"original request credential");
  int peer=-1;socklen_t peerlen=sizeof peer;check(!getsockopt(fd,SOL_SOCKET,77,&peer,&peerlen),"original peerpidfd");struct statfs fs;check(!fstatfs(peer,&fs)&&fs.f_type==0x50494446,"exact Guest PIDFS");close(peer);packets++;
  if(mode==0)check(send(fd,ack,16,MSG_NOSIGNAL)==16,"complete ack");
  if(mode==1){unsigned char bad[16];memcpy(bad,ack,16);bad[15]=4;check(send(fd,bad,16,MSG_NOSIGNAL)==16,"wrong action ack");}
  if(mode==2)check(send(fd,ack,7,MSG_NOSIGNAL)==7,"partial ack");
  if(mode==3){int ordinary=open("/dev/null",O_RDONLY|O_CLOEXEC);check(ordinary>=0,"rights original");unsigned char ancillary[CMSG_SPACE(sizeof(int))]={0};struct iovec a={(void*)ack,16};struct msghdr rights={.msg_iov=&a,.msg_iovlen=1,.msg_control=ancillary,.msg_controllen=sizeof ancillary};struct cmsghdr *c=CMSG_FIRSTHDR(&rights);c->cmsg_level=SOL_SOCKET;c->cmsg_type=SCM_RIGHTS;c->cmsg_len=CMSG_LEN(sizeof(int));memcpy(CMSG_DATA(c),&ordinary,sizeof ordinary);check(sendmsg(fd,&rights,MSG_NOSIGNAL)==16,"rights ack");close(ordinary);}
  /* mode4只关闭一次连接，不发送ACK；客户端必须Unknown且不重连。 */
  close(fd);
 }
 int status=wait_child(child);check(mode==0?status==0:status!=0,"actual CLI status");
 poller.revents=0;check(poll(&poller,1,150)==0,"client retried unknown request");close(listener);check(!unlink(path),"owned model listener cleanup");
 printf("DEVICE_CHECK_CASE %s packets=%d exit=%d\n",name,packets,status);fflush(stdout);
}
int main(void){
 check(getuid()==0&&geteuid()==0,"Guest Root only");umask(022);check(!mkdir("/run/neon-dsga",0755),"fresh private Guest parent");
 testcase(0,"exact-root-ack");testcase(1,"wrong-ack");testcase(2,"partial-ack");testcase(3,"rights-ack");testcase(4,"lost-ack-no-retry");testcase(5,"wrong-role-uid");testcase(6,"suid-root-refused");testcase(7,"wrong-socket-mode");
 puts("DEVICE_CHECK_NATIVE_JSON {\"cases\":8,\"requests\":5,\"positive\":1,\"negative\":7,\"Source\":false,\"normal\":false,\"Root_responder\":\"MODEL_ONLY\"}");return 0;
}
