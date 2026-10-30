#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo PG17_ISOLATED_INIT_BEGIN
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mkdir -p /sys/fs/cgroup
mount -t cgroup2 none /sys/fs/cgroup
echo 512 >/proc/sys/kernel/pid_max
echo +memory >/sys/fs/cgroup/cgroup.subtree_control
mkdir /sys/fs/cgroup/pg17-native-empty
export PG17_NATIVE_KERNEL_COMPONENT=isolated-kvm-component-only-v1
export PG17_NATIVE_EMPTY_PG_CGROUP=/sys/fs/cgroup/pg17-native-empty
export PG17_NATIVE_KERNEL_SHA256=0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0
set +e
/physical-kernel-native.test -test.run '^TestPG17NativePhysicalKernelComponent$' -test.v -test.count=1 -test.timeout=45s
result=$?
set -e
echo PG17_NATIVE_TEST_EXIT=$result
sync
poweroff -f
