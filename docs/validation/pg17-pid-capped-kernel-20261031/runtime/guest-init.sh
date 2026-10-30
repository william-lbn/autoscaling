#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
echo PG17_PID_INIT_BEGIN
/bin/busybox sh -c 'echo PG17_DIRECT_USER_EXEC=success'
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t securityfs securityfs /sys/kernel/security
chmod 0755 /tmp
expected=missing
capabilities=0
for token in $(cat /proc/cmdline); do
  case "$token" in
    pg17_pid_expected=*) expected=${token#pg17_pid_expected=} ;;
    pg17_pid_capabilities=1) capabilities=1 ;;
  esac
done
test "$expected" != missing
printf 'PG17_PID_RELEASE='; uname -r
printf 'PG17_PID_NOTES_SHA256='; sha256sum /sys/kernel/notes | cut -d ' ' -f 1
echo PG17_PID_NOTES_BASE64_BEGIN; base64 /sys/kernel/notes; echo PG17_PID_NOTES_BASE64_END
printf 'PG17_PID_LSM='; cat /sys/kernel/security/lsm; echo
printf 'PG17_PID_BOOT_ID='; cat /proc/sys/kernel/random/boot_id
printf 'PG17_PID_CMDLINE='; cat /proc/cmdline
printf 'PG17_PID_POSSIBLE_CPUS='; cat /sys/devices/system/cpu/possible
printf 'PG17_PID_BOOT_HARD='; cat /proc/sys/kernel/neonvm_pid_max_limit
printf 'PG17_PID_BOOT_LIVE='; cat /proc/sys/kernel/pid_max
set +e
/pid-probe "$expected"
probe=$?
set -e
echo PG17_PID_PROBE_EXIT=$probe
overall=$probe
if test "$capabilities" -eq 1; then
  test ! -e /umh-helper-marker
  printf '/umh-helper.sh' >/sys/kernel/uevent_helper
  printf 'PG17_PID_UMH_READBACK='; cat /sys/kernel/uevent_helper
  set +e
  echo add >/sys/devices/virtual/mem/null/uevent
  trigger=$?
  set -e
  echo PG17_PID_UMH_TRIGGER_EXIT=$trigger
  i=0
  while test "$i" -lt 50 && test ! -e /umh-helper-marker; do
    usleep 100000
    i=$((i+1))
  done
  if test -e /umh-helper-marker; then
    echo PG17_PID_UMH_MARKER=present
    cat /umh-helper-marker
    overall=6
  else
    echo PG17_PID_UMH_MARKER=absent
  fi
  if test "$trigger" -ne 0; then overall=$trigger; fi
  mkdir /tmp/landlock-allow /tmp/landlock-deny
  printf 'allow\n' >/tmp/landlock-allow/payload
  printf 'deny\n' >/tmp/landlock-deny/payload
  set +e
  /landlock-probe
  landlock=$?
  set -e
  echo PG17_PID_LANDLOCK_EXIT=$landlock
  if test "$landlock" -ne 0; then overall=$landlock; fi
fi
echo PG17_PID_GUEST_EXIT=$overall
sync
poweroff -f
