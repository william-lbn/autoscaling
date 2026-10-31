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
cat /sys/devices/system/memory/online
cat /sys/devices/system/node/online
echo MEMORY_ONLINE_END
echo EXISTING_VIRTIO_GEOMETRY_GUEST_EXIT=0
sync
poweroff -f
