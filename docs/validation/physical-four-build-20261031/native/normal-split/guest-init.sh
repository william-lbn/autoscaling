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
export DSGA_NATIVE_REVIEWED_KERNEL_PROFILE=neon-linux-6.12.26-dsga-normal-split-v1
export DSGA_NATIVE_REVIEWED_KERNEL_SHA256=3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0
export DSGA_NATIVE_REVIEWED_CONFIG_SHA256=11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be
set +e
/reviewed-physical-native.test -test.run '^TestNativeReviewedPhysicalKernelComponent$' -test.v -test.count=1 -test.timeout=60s
result=$?
set -e
echo DSGA_REVIEWED_READER_GUEST_EXIT=$result
sync
poweroff -f
