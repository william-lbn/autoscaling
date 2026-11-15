#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/reboot.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <unistd.h>

static const size_t huge_size = 2 * 1024 * 1024;

struct pool {
    unsigned long total, free, reserved, surplus;
};

static void finish(const char *result)
{
    printf("DSGA_HUGETLB_RESULT=%s\n", result);
    fflush(stdout);
    sync();
    reboot(RB_POWER_OFF);
    for (;;)
        pause();
}

static void require(bool valid, const char *step)
{
    if (valid)
        return;
    printf("DSGA_HUGETLB_ERROR=%s errno=%d\n", step, errno);
    finish("failed");
}

static struct pool observe(const char *stage)
{
    struct pool sample = {0};
    unsigned found = 0;
    char line[160];
    FILE *info = fopen("/proc/meminfo", "r");
    require(info != NULL, "open meminfo");
    while (fgets(line, sizeof(line), info)) {
        if (sscanf(line, "HugePages_Total: %lu", &sample.total) == 1)
            found |= 1;
        else if (sscanf(line, "HugePages_Free: %lu", &sample.free) == 1)
            found |= 2;
        else if (sscanf(line, "HugePages_Rsvd: %lu", &sample.reserved) == 1)
            found |= 4;
        else if (sscanf(line, "HugePages_Surp: %lu", &sample.surplus) == 1)
            found |= 8;
    }
    require(fclose(info) == 0 && found == 15, "read complete pool");
    printf("DSGA_POOL={\"stage\":\"%s\",\"total\":%lu,\"free\":%lu,\"reserved\":%lu,\"surplus\":%lu}\n",
           stage, sample.total, sample.free, sample.reserved, sample.surplus);
    return sample;
}

static volatile unsigned char *map_shared(int fd, size_t bytes)
{
    void *buffer = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    require(buffer != MAP_FAILED, "mmap shared backing");
    return buffer;
}

static int remove_range(volatile unsigned char *buffer, size_t bytes, const char *stage)
{
    errno = 0;
    int removed = madvise((void *)buffer, bytes, MADV_REMOVE);
    int error = errno;
    printf("DSGA_REMOVE={\"stage\":\"%s\",\"bytes\":%zu,\"result\":%d,\"errno\":%d}\n",
           stage, bytes, removed, error);
    return removed;
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (getpid() != 1 || geteuid() != 0 || mkdir("/proc", 0755) != 0 || mount("proc", "/proc", "proc", 0, NULL) != 0) {
        fprintf(stderr, "The probe requires the isolated Guest initramfs\n");
        return EXIT_FAILURE;
    }
    char cmdline[4096];
    FILE *boot = fopen("/proc/cmdline", "r");
    if (boot == NULL || fgets(cmdline, sizeof(cmdline), boot) == NULL || fclose(boot) != 0 ||
            strstr(cmdline, " dsga_c6_guest_probe=1 ") == NULL) {
        fprintf(stderr, "The isolated Guest boot marker is missing\n");
        return EXIT_FAILURE;
    }
    require(mkdir("/sys", 0755) == 0 && mount("sysfs", "/sys", "sysfs", 0, NULL) == 0, "mount sysfs");
    require(mkdir("/huge", 0700) == 0 && mount("none", "/huge", "hugetlbfs", 0, "pagesize=2M") == 0,
            "mount Guest hugetlbfs");
    struct utsname kernel;
    require(uname(&kernel) == 0, "read Guest kernel version");
    printf("DSGA_GUEST_KERNEL=%s\n", kernel.release);
    struct pool initial = observe("boot");
    require(initial.total == 4 && initial.free == 4 && initial.surplus == 0, "isolated boot pool");

    const size_t ordinary_size = (size_t)sysconf(_SC_PAGESIZE);
    int ordinary = memfd_create("ordinary", MFD_CLOEXEC);
    require(ordinary >= 0 && ftruncate(ordinary, ordinary_size) == 0, "ordinary memfd");
    volatile unsigned char *plain = map_shared(ordinary, ordinary_size);
    volatile unsigned char *plain_alias = map_shared(ordinary, ordinary_size);
    plain[0] = 99;
    require(plain_alias[0] == 99 && remove_range(plain, ordinary_size, "ordinary-shmem") == 0 && plain_alias[0] == 0,
            "ordinary shmem hole visible through alias");
    require(munmap((void *)plain, ordinary_size) == 0 && munmap((void *)plain_alias, ordinary_size) == 0 && close(ordinary) == 0,
            "close ordinary memfd");

    int fd = open("/huge/fixture", O_RDWR | O_CREAT | O_EXCL, 0600);
    require(fd >= 0 && ftruncate(fd, 2 * huge_size) == 0, "open two-page Guest backing");
    volatile unsigned char *huge = map_shared(fd, 2 * huge_size);
    volatile unsigned char *alias = map_shared(fd, 2 * huge_size);
    huge[0] = 11;
    huge[huge_size] = 33;
    require(alias[0] == 11 && alias[huge_size] == 33, "settle both aliases");
    struct pool populated = observe("faulted-two-pages");
    require(populated.total == 4 && populated.free == 2 && populated.surplus == 0, "two pages charged in Guest");

    remove_range(huge, ordinary_size, "partial-huge-page");
    observe("after-partial-before-refault");
    printf("DSGA_ALIAS={\"stage\":\"partial\",\"first\":%u,\"second\":%u}\n", alias[0], alias[huge_size]);
    require(alias[huge_size] == 33, "partial operation leaves adjacent huge page intact");

    remove_range(huge + huge_size, huge_size, "aligned-huge-page");
    observe("after-aligned-before-refault");
    printf("DSGA_ALIAS={\"stage\":\"aligned\",\"first\":%u,\"second\":%u}\n", alias[0], alias[huge_size]);
    require(munmap((void *)huge, 2 * huge_size) == 0 && munmap((void *)alias, 2 * huge_size) == 0 &&
            close(fd) == 0 && unlink("/huge/fixture") == 0, "retire mappings and original file");
    struct pool released = observe("after-backing-retirement");
    require(released.total == 4 && released.free == 4 && released.reserved == 0 && released.surplus == 0,
            "retire to the original Guest pool");

    int pool_fd = open("/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages", O_WRONLY);
    require(pool_fd >= 0 && write(pool_fd, "0\n", 2) == 2 && close(pool_fd) == 0, "return idle pool to Guest page allocator");
    struct pool final = observe("after-pool-shrink");
    require(final.total == 0 && final.free == 0 && final.reserved == 0 && final.surplus == 0, "observe retired Guest pool");
    finish("passed");
}
