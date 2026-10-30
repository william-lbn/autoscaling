#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo PG17_NORMAL_READER_INIT_BEGIN
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t securityfs securityfs /sys/kernel/security
mkdir -p /sys/fs/cgroup
mount -t cgroup2 none /sys/fs/cgroup
chmod 0755 /tmp
echo 512 >/proc/sys/kernel/pid_max
echo +memory >/sys/fs/cgroup/cgroup.subtree_control
mkdir /sys/fs/cgroup/neon-postgres
export PG17_NATIVE_NORMAL_KERNEL_COMPONENT=isolated-kvm-normal-reader-v1
export PG17_NATIVE_NORMAL_KERNEL_SHA256=552ffabd096736e38d2d4b6c119533e4a26c2b9a47f6e04a8e1795a084f8d85d
export PG17_NATIVE_NORMAL_CONFIG_SHA256=935e43773879d6a4f5c27c2ef966314de7e0e463a4a9bebefa3dc5dcf283ce5b
set +e
/normal-reader-native.test -test.run '^TestPG17NativeNormalKernelComponent$' -test.v -test.count=1 -test.timeout=60s
result=$?
set -e
echo PG17_NORMAL_READER_TEST_EXIT=$result
sync
poweroff -f
