#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
mount -t proc proc /proc
mount -t sysfs sysfs /sys
echo KERNEL_NOTES_BEGIN
base64 /sys/kernel/notes
echo KERNEL_NOTES_END
mount -t tmpfs -o size=512m,mode=0755 system-fixture /newroot
cp -a /system-fixture/. /newroot/
cp /system-source.test /newroot/system-source.test
mkdir -p /newroot/proc /newroot/sys /newroot/dev /newroot/neonvm/runtime
mount --bind /proc /newroot/proc
mount --bind /sys /newroot/sys
mount --bind /dev /newroot/dev
mount -t tmpfs -o size=1m,mode=0755,ro source-runtime /newroot/neonvm/runtime
mount -o remount,ro /newroot
export DSGA_NATIVE_SYSTEM_SOURCE=isolated-369-system-source-v1
set +e
chroot /newroot /system-source.test -test.run '^TestNativeNormalSystemV2Component$' -test.v -test.count=1 -test.timeout=45s
result=$?
set -e
echo SYSTEM_SOURCE_GUEST_EXIT=$result
sync
poweroff -f
