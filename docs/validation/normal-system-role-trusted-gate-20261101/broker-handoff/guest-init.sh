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
printf 67108864 >/sys/fs/cgroup/neon-system/memory.max
printf 0 >/sys/fs/cgroup/neon-system/memory.swap.max
printf 128 >/sys/fs/cgroup/neon-system/pids.max
mkdir -p /neonvm/runtime
mount -t iso9660 -o ro /dev/vdb /neonvm/runtime || fail 98
mount -t ext4 -o noatime /dev/vda /var/lib/neon-dsga/state || fail 96
chmod 700 /var/lib/neon-dsga/state
mount -t tmpfs -o size=16m,nodev,nosuid tmpfs /tmp || fail 99
mount --bind / /
mount -o remount,bind,ro / /
printf 'PG17_SPLIT_RELEASE='; uname -r
printf 'PG17_SPLIT_NOTES_SHA256='; sha256sum /sys/kernel/notes | cut -d ' ' -f 1
echo PG17_SPLIT_NOTES_BASE64_BEGIN; base64 /sys/kernel/notes; echo PG17_SPLIT_NOTES_BASE64_END
printf 'PG17_SPLIT_BINARY_SHA256='; sha256sum /lifecycle.test | cut -d ' ' -f 1
printf 'PG17_SPLIT_CMDLINE='; cat /proc/cmdline
printf 'PG17_SPLIT_BOOT_ID='; cat /proc/sys/kernel/random/boot_id
printf "%s" "$$" >/sys/fs/cgroup/neon-system/cgroup.procs
/bin/busybox env -i PATH=/neonvm/bin /neonvm/bin/dsga-resource-guard initial-boot
rc=$?
if test "$rc" = 0; then
 for i in 1 2 3 4 5 6 7 8 9 10; do test -s /tmp/normal-broker-pid1-proof.json && break; sleep 1; done
 test -s /tmp/normal-broker-pid1-proof.json || fail 77
 sleep 1
fi
echo PG17_SPLIT_TEST_EXIT=$rc
sync
umount /var/lib/neon-dsga/state || fail 97
echo PG17_SPLIT_GUEST_EXIT=$rc
poweroff -f
