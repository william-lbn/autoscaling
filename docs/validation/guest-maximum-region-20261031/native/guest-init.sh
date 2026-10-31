#!/bin/busybox sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
mount -t devtmpfs devtmpfs /dev
exec </dev/console >/dev/console 2>&1
mount -t proc proc /proc
mount -t sysfs sysfs /sys
echo EXISTING_VIRTIO_GEOMETRY_BEGIN
cat /proc/sys/kernel/osrelease
echo KERNEL_NOTES_BEGIN
base64 /sys/kernel/notes
echo KERNEL_NOTES_END
echo IOMEM_BEGIN
cat /proc/iomem
echo IOMEM_END
for device in /sys/bus/virtio/devices/*; do
 echo VIRTIO_DEVICE_BEGIN
 readlink -f "$device"
 cat "$device/device" "$device/vendor" "$device/status" "$device/features"
 readlink -f "$device/driver"
 ls -la "$device/"
 echo VIRTIO_DEVICE_END
done
echo MEMORY_ONLINE_BEGIN
for block in /sys/devices/system/memory/memory*/state; do
 printf '%s=' "$block"
 cat "$block"
done
cat /sys/devices/system/node/online
echo MEMORY_ONLINE_END
export DSGA_NATIVE_GUEST_MAXIMUM_COMPONENT=isolated-369-readonly-region-v1
export DSGA_NATIVE_GUEST_MAXIMUM_KERNEL_SHA256=3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0
export DSGA_NATIVE_GUEST_MAXIMUM_CONFIG_SHA256=11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be
set +e
/guest-maximum-native.test -test.run '^TestNativeGuestMemoryMaximumComponent$' -test.v -test.count=1 -test.timeout=30s
result=$?
set -e
echo EXISTING_VIRTIO_GEOMETRY_GUEST_EXIT=$result
sync
poweroff -f
