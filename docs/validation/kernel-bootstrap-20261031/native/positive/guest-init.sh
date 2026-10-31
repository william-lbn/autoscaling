#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo KERNEL_BOOTSTRAP_NATIVE_INIT
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t securityfs securityfs /sys/kernel/security
mount -t cgroup2 none /sys/fs/cgroup
echo +memory >/sys/fs/cgroup/cgroup.subtree_control
mkdir /sys/fs/cgroup/neon-postgres
mount -t ext4 /dev/vda /var/lib/neon-dsga
mount -t iso9660 -o ro /dev/vdb /neonvm/runtime
:
export DSGA_KERNEL_BOOTSTRAP_NATIVE=isolated-kvm-kernel-bootstrap-v1
export DSGA_KERNEL_BOOTSTRAP_MODE=positive
set +e
/kernel-bootstrap-native.test -test.run '^TestNativeKernelBootstrapComponent$' -test.v -test.count=1 -test.timeout=60s
rc=$?
set -e
echo KERNEL_BOOTSTRAP_NATIVE_GUEST_EXIT=$rc
sync
poweroff -f
