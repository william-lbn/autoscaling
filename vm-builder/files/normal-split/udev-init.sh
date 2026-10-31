#!/neonvm/bin/sh
# 不假定固定vport编号，且绝不把启动通道当普通可写日志口。
set -eu
/neonvm/bin/dsga-resource-guard check-guest >/dev/null || exit 1
/neonvm/bin/busybox mkdir -p /dev/virtio-ports
seen=0
for p in /sys/class/virtio-ports/*; do
 [ -r "$p/name" ] || continue
 name=$(/neonvm/bin/busybox cat "$p/name")
 [ "$name" = tech.neon.log.0 ] || continue
 [ "$seen" = 0 ] || exit 1
 seen=1
 dev=$(/neonvm/bin/busybox cat "$p/dev")
 major=${dev%:*}; minor=${dev#*:}
 case "$major:$minor" in *[!0-9:]*|:*) exit 1 ;; esac
 [ -e /dev/virtio-ports/tech.neon.log.0 ] || /neonvm/bin/busybox mknod /dev/virtio-ports/tech.neon.log.0 c "$major" "$minor"
 /neonvm/bin/busybox chmod 0666 /dev/virtio-ports/tech.neon.log.0
done
