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
export DSGA_NATIVE_REVIEWED_KERNEL_PROFILE=neon-linux-6.12.26-dsga-normal-split-metadata-v1
export DSGA_NATIVE_REVIEWED_KERNEL_SHA256=18c9fd3a38bbe5a087b98998c856898e97ee3f0bc68681b13f0287912849e1ad
export DSGA_NATIVE_REVIEWED_CONFIG_SHA256=966e615127358d6d0dcf4f6ca884a07702e957ad07a95909cf95feca4aae5441
set +e
/reviewed-physical-native.test -test.run '^TestNativeReviewedPhysicalKernelComponent$' -test.v -test.count=1 -test.timeout=60s
result=$?
set -e
echo DSGA_REVIEWED_READER_GUEST_EXIT=$result
sync
poweroff -f
