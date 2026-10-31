#!/bin/busybox sh
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
fail() { echo PG17_SPLIT_GUEST_EXIT=$1; poweroff -f; while true; do sleep 1; done; }
mount -t proc proc /proc || fail 90
mount -t sysfs sysfs /sys || fail 91
mount -t securityfs none /sys/kernel/security || fail 92
mount -t cgroup2 none /sys/fs/cgroup || fail 93
printf '+memory +pids' > /sys/fs/cgroup/cgroup.subtree_control || fail 94
mkdir /sys/fs/cgroup/neon-postgres /sys/fs/cgroup/neon-system || fail 95
mount -t ext4 -o noatime /dev/vda /var/lib/neon-dsga/state || fail 96
chmod 700 /var/lib/neon-dsga/state
printf 'PG17_SPLIT_RELEASE='; uname -r
printf 'PG17_SPLIT_NOTES_SHA256='; sha256sum /sys/kernel/notes | cut -d ' ' -f 1
echo PG17_SPLIT_NOTES_BASE64_BEGIN; base64 /sys/kernel/notes; echo PG17_SPLIT_NOTES_BASE64_END
printf 'PG17_SPLIT_BINARY_SHA256='; sha256sum /topology.test | cut -d ' ' -f 1
printf 'PG17_SPLIT_CMDLINE='; cat /proc/cmdline
printf 'PG17_SPLIT_BOOT_ID='; cat /proc/sys/kernel/random/boot_id
PG17_NATIVE_SPLIT_TOPOLOGY=isolated-369-kvm-component-v1 /topology.test -test.v -test.run '^TestNormalSplitTopologyActualGuestKernel$' -test.count=1 -test.timeout=45s
rc=$?
echo PG17_SPLIT_TEST_EXIT=$rc
sync
umount /var/lib/neon-dsga/state || fail 97
echo PG17_SPLIT_GUEST_EXIT=$rc
poweroff -f
