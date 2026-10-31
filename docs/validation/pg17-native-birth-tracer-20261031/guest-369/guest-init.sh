#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t cgroup2 none /sys/fs/cgroup
chmod 1777 /tmp
mount -o bind /payload/pg /pgroot
mount -o remount,bind,ro /payload/pg /pgroot
echo PG17_TRACER_RELEASE=$(uname -r)
echo PG17_TRACER_NOTES_SHA256=$(sha256sum /sys/kernel/notes | cut -d' ' -f1)
echo PG17_TRACER_HARDCAP=$(cat /proc/sys/kernel/neonvm_pid_max_limit)
echo PG17_TRACER_BINARY_SHA256=$(sha256sum /probe/tracer.test | cut -d' ' -f1)
test "$(uname -r)" = 6.12.26-william-normal-split-v1-e210f5e3ee94
test "$(sha256sum /sys/kernel/notes | cut -d' ' -f1)" = 7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828
test "$(cat /proc/sys/kernel/neonvm_pid_max_limit)" = 512
test "$(cat /proc/sys/kernel/pid_max)" = 512
export PG17_NATIVE_BIRTH_TRACER=isolated-pg17-root-tracer-v1
export PG17_TRACE_PG=/pgroot/bin/postgres
export PG17_TRACE_PG_SHA256=c990ed4251edb270d37894036c70d90e012fea2bb441a7a48faeb0ed6375be53
export PG17_TRACE_EVIDENCE=/evidence
export PG17_TRACE_PREBUILT_LAUNCHER=/probe/nnp-launcher
export PG17_TRACE_LAUNCHER_SOURCE_SHA256=fc7b493b50e159aa27d94a6f947689db6bebc519ac4a0af880cf8feab548b5e6
export PG17_TRACE_LAUNCHER_SHA256=d908a89d467e65b4ea01b9e52a8bb82559463b3aa82df8e293359481431d696a
export PG17_TRACE_FORCE_PID_REUSE=isolated369-initial-pidns-v1
set +e
/probe/tracer.test -test.run='^TestNativeBirthTracerKernel' -test.count=1 -test.v -test.timeout=120s
result=$?
set -e
echo PG17_TRACER_GUEST_EXIT=$result
echo 0 > /proc/sys/kernel/printk
for file in /evidence/*.json; do
 test -f "$file" || continue
 name=$(basename "$file")
 sha=$(sha256sum "$file" | cut -d' ' -f1)
 blob=$(base64 "$file" | tr -d '\n')
 echo "PG17_TRACER_FILE=$name:$sha:$blob"
done
sync
sleep 2
poweroff -f
