#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo KERNEL_PREP_V2_NATIVE_INIT
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t securityfs securityfs /sys/kernel/security
mount -t cgroup2 none /sys/fs/cgroup
echo +memory >/sys/fs/cgroup/cgroup.subtree_control
mkdir /sys/fs/cgroup/neon-postgres
mount -t ext4 /dev/vda /var/lib/neon-dsga
mount -t iso9660 -o ro /dev/vdb /neonvm/runtime
echo V2_KERNEL_RELEASE=$(uname -r)
echo V2_KERNEL_NOTES=$(sha256sum /sys/kernel/notes | cut -d' ' -f1)
echo V2_ACTIVE_LSM=$(cat /sys/kernel/security/lsm)
export DSGA_KERNEL_PREP_V2_NATIVE=isolated18c-userns-v2
export DSGA_KERNEL_PREP_V2_MODE=positive
export GOMAXPROCS=2
export GOMEMLIMIT=128MiB
set +e
/kernel-prep-v2.test -test.run '^TestNativeKernelPrepUserNSV2Component$' -test.v -test.count=1 -test.timeout=100s
rc=$?
set -e
echo KERNEL_PREP_V2_GUEST_EXIT=$rc
sync
poweroff -f
