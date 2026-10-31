#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo DSGA_REVIEWED_READER_INIT_BEGIN
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t securityfs securityfs /sys/kernel/security
mount -t cgroup2 none /sys/fs/cgroup
echo +memory >/sys/fs/cgroup/cgroup.subtree_control
mkdir /sys/fs/cgroup/neon-postgres
export DSGA_NATIVE_REVIEWED_KERNEL_COMPONENT=isolated-kvm-reviewed-reader-v1
export DSGA_NATIVE_REVIEWED_KERNEL_PROFILE=pg17-linux-6.12.26-normal-v1-pid-capped
export DSGA_NATIVE_REVIEWED_KERNEL_SHA256=3b5853b552a8adbe5de7772410c793928f0d4a86a2410d24aec806fecfea2608
export DSGA_NATIVE_REVIEWED_CONFIG_SHA256=7733d4e430a2102052dd97c8b3fa64ac2b5904fa6a4f394b5653b9fa5cccbf25
set +e
/reviewed-physical-native.test -test.run '^TestNativeReviewedPhysicalKernelComponent$' -test.v -test.count=1 -test.timeout=60s
result=$?
set -e
echo DSGA_REVIEWED_READER_GUEST_EXIT=$result
sync
poweroff -f
