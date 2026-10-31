#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -o bind / /
mount -o remount,bind,ro / /
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t cgroup2 none /sys/fs/cgroup
mount -t tmpfs -o size=128m,mode=1777 tmpfs /tmp
mount -t tmpfs -o size=8m,mode=755 tmpfs /evidence
mount -t ext4 /dev/vda /state
mount -o bind /payload/helper /neonvm/bin
mount -o remount,bind,ro /payload/helper /neonvm/bin
echo '+memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
mkdir /sys/fs/cgroup/neon-postgres /sys/fs/cgroup/neon-system
echo 335544320 > /sys/fs/cgroup/neon-postgres/memory.max
echo 0 > /sys/fs/cgroup/neon-postgres/memory.swap.max
echo '+memory +pids' >/sys/fs/cgroup/neon-postgres/cgroup.subtree_control
mkdir /sys/fs/cgroup/neon-postgres/private /sys/fs/cgroup/neon-postgres/arrays
for leaf in private arrays; do
 echo 134217728 > /sys/fs/cgroup/neon-postgres/$leaf/memory.max
 echo 0 > /sys/fs/cgroup/neon-postgres/$leaf/memory.swap.max
done
echo 1 > /sys/fs/cgroup/neon-postgres/arrays/pids.max
echo 536870912 > /sys/fs/cgroup/neon-system/memory.max
echo 0 > /sys/fs/cgroup/neon-system/memory.swap.max
echo $$ > /sys/fs/cgroup/neon-system/cgroup.procs
echo ARRAY_HELPER_RELEASE=$(uname -r)
echo ARRAY_HELPER_NOTES=$(sha256sum /sys/kernel/notes | cut -d' ' -f1)
export DSGA_ARRAY_HELPER_NATIVE=isolated369-array-helper-v1
export DSGA_ARRAY_HELPER_ELF_SHA256=a186e9e072f052d33fb6598d6a8fc81235eef093ce9ef7697684dc3a5b5af157
export DSGA_ARRAY_HELPER_EVIDENCE=/evidence
set +e
/array-helper.test -test.run '^TestNativeArrayPopulateKernelComponent$' -test.count=1 -test.v -test.timeout=60s
rc=$?
set -e
echo ARRAY_HELPER_GUEST_EXIT=$rc
for file in /evidence/*.json; do
 test -f "$file" || continue
 name=$(basename "$file")
 sha=$(sha256sum "$file" | cut -d' ' -f1)
 blob=$(base64 "$file" | tr -d '\n')
 echo "ARRAY_HELPER_FILE=$name:$sha:$blob"
done
for leaf in neon-postgres neon-postgres/arrays neon-system; do
 echo ARRAY_HELPER_COUNTER_BEGIN=$leaf
 cat /sys/fs/cgroup/$leaf/memory.current /sys/fs/cgroup/$leaf/memory.events.local
 echo ARRAY_HELPER_COUNTER_END
done
sync
poweroff -f
