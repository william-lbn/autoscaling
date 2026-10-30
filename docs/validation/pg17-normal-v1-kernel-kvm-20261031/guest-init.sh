#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo PG17_VARIANT_INIT_BEGIN
/bin/busybox sh -c 'echo PG17_DIRECT_USER_EXEC=success'
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t securityfs securityfs /sys/kernel/security
mkdir -p /sys/fs/cgroup
mount -t cgroup2 none /sys/fs/cgroup
chmod 0755 /tmp
echo 512 >/proc/sys/kernel/pid_max
printf 'PG17_VARIANT_RELEASE='; uname -r
printf 'PG17_VARIANT_NOTES_SHA256='; sha256sum /sys/kernel/notes | cut -d ' ' -f 1
echo PG17_VARIANT_NOTES_BASE64_BEGIN; base64 /sys/kernel/notes; echo PG17_VARIANT_NOTES_BASE64_END
printf 'PG17_VARIANT_LSM='; cat /sys/kernel/security/lsm
printf 'PG17_VARIANT_BOOT_ID='; cat /proc/sys/kernel/random/boot_id
printf 'PG17_VARIANT_CMDLINE='; cat /proc/cmdline
printf 'PG17_VARIANT_POSSIBLE_CPUS='; cat /sys/devices/system/cpu/possible
printf 'PG17_VARIANT_ONLINE_POLICY='; cat /sys/devices/system/memory/auto_online_blocks
echo PG17_VARIANT_ZONEINFO_BEGIN; cat /proc/zoneinfo; echo PG17_VARIANT_ZONEINFO_END
export PG17_NATIVE_BIRTH_WIRE=isolated-seqpacket-component-v1
set +e
/wire-kernel-native.test -test.run '^TestNativeBirthWireKernelPeerAndMalformedFrames$' -test.v -test.count=1 -test.timeout=90s
wire=$?
set -e
echo PG17_VARIANT_WIRE_EXIT=$wire
test ! -e /umh-helper-marker
printf '/umh-helper.sh' >/sys/kernel/uevent_helper
printf 'PG17_VARIANT_UMH_READBACK='; cat /sys/kernel/uevent_helper
set +e
echo add >/sys/devices/virtual/mem/null/uevent
trigger=$?
set -e
echo PG17_VARIANT_UMH_TRIGGER_EXIT=$trigger
i=0
while test "$i" -lt 50 && test ! -e /umh-helper-marker; do
  usleep 100000
  i=$((i+1))
done
if test -e /umh-helper-marker; then
  echo PG17_VARIANT_UMH_MARKER=present
  cat /umh-helper-marker
else
  echo PG17_VARIANT_UMH_MARKER=absent
fi
mkdir /tmp/landlock-allow /tmp/landlock-deny
printf 'allow\n' >/tmp/landlock-allow/payload
printf 'deny\n' >/tmp/landlock-deny/payload
set +e
/landlock-probe
landlock=$?
set -e
echo PG17_VARIANT_LANDLOCK_EXIT=$landlock
overall=$landlock
if test "$wire" -ne 0; then overall=$wire; fi
if test "$trigger" -ne 0; then overall=$trigger; fi
echo PG17_VARIANT_GUEST_EXIT=$overall
sync
poweroff -f
