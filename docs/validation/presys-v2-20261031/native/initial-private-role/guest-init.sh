#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -o bind / /
mount -o remount,bind,ro / /
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo PRESYS_NATIVE_INIT
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t securityfs securityfs /sys/kernel/security
mount -t cgroup2 none /sys/fs/cgroup
echo "+memory +pids" >/sys/fs/cgroup/cgroup.subtree_control
mkdir /sys/fs/cgroup/neon-postgres /sys/fs/cgroup/neon-system
mount -t ext4 /dev/vda /var/lib/neon-dsga
mount -t iso9660 -o ro /dev/vdb /neonvm/runtime
:
export DSGA_PRESYS_NATIVE=isolated-kvm-presys-v2
export DSGA_PRESYS_MODE=initial-private-role
set +e
/presys-native.test -test.run '^TestNativePreSystemSplitComponent$' -test.v -test.count=1 -test.timeout=60s
rc=$?
set -e
echo PRESYS_NATIVE_GUEST_EXIT=$rc
sync
poweroff -f
