#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo PG17_COUNTER_INIT_BEGIN
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t cgroup2 none /sys/fs/cgroup
printf 'PG17_COUNTER_RELEASE='; uname -r
printf 'PG17_COUNTER_NOTES_SHA256='; sha256sum /sys/kernel/notes | cut -d ' ' -f 1
echo PG17_COUNTER_NOTES_BASE64_BEGIN; base64 /sys/kernel/notes; echo PG17_COUNTER_NOTES_BASE64_END
printf 'PG17_COUNTER_CMDLINE='; cat /proc/cmdline
printf 'PG17_COUNTER_POSSIBLE_CPUS='; cat /sys/devices/system/cpu/possible
printf 'PG17_COUNTER_HARD_PID='; cat /proc/sys/kernel/neonvm_pid_max_limit
mode=pressure
for token in $(cat /proc/cmdline); do case "$token" in counter_mode=irq) mode=irq ;; esac; done
set +e
/counter-probe "$mode"
code=$?
set -e
echo PG17_COUNTER_GUEST_EXIT=$code
sync
poweroff -f
