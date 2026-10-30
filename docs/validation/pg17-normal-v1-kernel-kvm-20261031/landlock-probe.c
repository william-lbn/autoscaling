#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/landlock.h>
#include <stdio.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>

static int fail(const char *stage, int error, int abi) {
    printf("PG17_LANDLOCK_PROBE={\"scope\":\"single-process-capability-only\",\"abi\":%d,\"stage\":\"%s\",\"errno\":%d,\"enforced\":false,\"vm_shrink_permit\":false}\n", abi, stage, error);
    return 2;
}
int main(void) {
    errno = 0;
    int abi = syscall(__NR_landlock_create_ruleset, NULL, 0, LANDLOCK_CREATE_RULESET_VERSION);
    int saved = errno;
    if (abi < 1) return fail("query-ABI", saved, abi);
    int pre_allow = open("/tmp/landlock-allow/payload", O_RDONLY | O_CLOEXEC);
    int pre_deny = open("/tmp/landlock-deny/payload", O_RDONLY | O_CLOEXEC);
    if (pre_allow < 0 || pre_deny < 0) return fail("preflight-readable-files", errno, abi);
    close(pre_allow); close(pre_deny);
    struct landlock_ruleset_attr ruleset_attr = { .handled_access_fs = LANDLOCK_ACCESS_FS_READ_FILE };
    int ruleset = syscall(__NR_landlock_create_ruleset, &ruleset_attr, sizeof(ruleset_attr), 0);
    if (ruleset < 0) return fail("create-ruleset", errno, abi);
    int allowed = open("/tmp/landlock-allow", O_PATH | O_CLOEXEC);
    if (allowed < 0) { saved = errno; close(ruleset); return fail("open-allowed-parent", saved, abi); }
    struct landlock_path_beneath_attr rule = { .allowed_access = LANDLOCK_ACCESS_FS_READ_FILE, .parent_fd = allowed };
    if (syscall(__NR_landlock_add_rule, ruleset, LANDLOCK_RULE_PATH_BENEATH, &rule, 0)) {
        saved = errno; close(allowed); close(ruleset); return fail("add-rule", saved, abi);
    }
    close(allowed);
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) { saved = errno; close(ruleset); return fail("set-NNP", saved, abi); }
    if (syscall(__NR_landlock_restrict_self, ruleset, 0)) { saved = errno; close(ruleset); return fail("restrict-self", saved, abi); }
    close(ruleset);
    char content[16] = {0};
    int allow_fd = open("/tmp/landlock-allow/payload", O_RDONLY | O_CLOEXEC);
    ssize_t size = allow_fd >= 0 ? read(allow_fd, content, sizeof(content) - 1) : -1;
    int allow_ok = size == 6 && !memcmp(content, "allow\n", 6);
    if (allow_fd >= 0) close(allow_fd);
    errno = 0;
    int deny_fd = open("/tmp/landlock-deny/payload", O_RDONLY | O_CLOEXEC);
    int deny_error = errno;
    if (deny_fd >= 0) close(deny_fd);
    int nnp = prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0);
    int enforced = allow_ok && deny_fd < 0 && deny_error == EACCES && nnp == 1;
    printf("PG17_LANDLOCK_PROBE={\"scope\":\"single-process-capability-only\",\"abi\":%d,\"stage\":\"deny-allow-complete\",\"uid\":%u,\"page_size\":%ld,\"preflight_both_readable\":true,\"allowed_read\":%s,\"denied_read_errno\":%d,\"nnp\":%d,\"enforced\":%s,\"vm_shrink_permit\":false}\n",
           abi, (unsigned)getuid(), sysconf(_SC_PAGESIZE), allow_ok ? "true" : "false", deny_error, nnp, enforced ? "true" : "false");
    return enforced ? 0 : 1;
}
