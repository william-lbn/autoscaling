/* Neon internal privsep ABI. No authorization/Root Source is encoded here. */
#ifndef NEON_UDEV_PRIVSEP_PROTOCOL_H
#define NEON_UDEV_PRIVSEP_PROTOCOL_H
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
#define NEON_IPC_MAX 8192U
#define NEON_IPC_HEADER 40U
#define NEON_ROOT_HEADER 16U
#define NEON_EVENT_HEADER 40U
#define NEON_EVENT_MAX (NEON_IPC_MAX-NEON_EVENT_HEADER)
/* 仅原对象 selector；Root 仍须原票据和完整投影核验，不是授权。 */
#define NEON_DEVICE_LOG 1U
#define NEON_DEVICE_MEMORY 2U
#define NEON_DEVICE_STATIC 3U
#define NEON_EVENT 1U
#define NEON_DONE 2U
#define NEON_FAIL 3U
#define NEON_ROLE_UID 60004U
#define NEON_EMITTER_PATH "/run/neon-dsga/ordinary-udev.sock"
struct neon_endpoint {
    int fd, peer_pidfd;
    pid_t peer_pid, creator_pid;
    uint64_t cookie, device, inode, generation, seq;
    uint64_t peer_device, peer_inode, creator_device, creator_inode;
    int awaiting;
};
struct neon_pair {
    int fd[2], creator_pidfd;
    pid_t creator_pid;
    uint64_t cookie[2], device[2], inode[2];
    uint64_t creator_device, creator_inode;
};
static uint32_t neon_get32(const unsigned char *p) { return ((uint32_t)p[0]<<24)|((uint32_t)p[1]<<16)|((uint32_t)p[2]<<8)|p[3]; }
static uint64_t neon_get64(const unsigned char *p) { return ((uint64_t)neon_get32(p)<<32)|neon_get32(p+4); }
static void neon_put32(unsigned char *p,uint32_t n) { p[0]=n>>24;p[1]=n>>16;p[2]=n>>8;p[3]=n; }
static void neon_put64(unsigned char *p,uint64_t n) { neon_put32(p,n>>32);neon_put32(p+4,n); }
static void neon_put16(unsigned char *p,uint16_t n) { p[0]=n>>8;p[1]=n; }
#endif
