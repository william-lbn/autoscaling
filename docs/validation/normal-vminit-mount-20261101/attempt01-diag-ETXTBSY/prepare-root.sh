#!/neonvm/bin/sh
# 构建期、最终merge之后执行，不在Guest修改静态配置或库alias。
set -eu
[ "$#" -eq 2 ] || exit 1
case "$1:$2" in *[!0-9:]*|0:*|*:0) exit 1 ;; esac
root=/rootdisk
bb=/rootdisk/neonvm/bin/busybox
for p in /etc/vector /lib/udev/rules.d /etc/udev/rules.d /etc/neon-dsga /var/lib/neon-dsga /var/db/postgres /var/log/chrony /var/lib/chrony /run /tmp /dev/pts /dev/shm; do
 $bb mkdir -p "$root$p"
done
# 不碰已有动态扩展目录的ownership；只为原固定数据父目录落实显式计划身份。
$bb chmod 0755 "$root/etc/neon-dsga" "$root/var/lib/neon-dsga"
$bb chown "$1:$2" "$root/var/db/postgres"
# resolv.conf是动态网络孔，必须是真实普通文件，不跟随源镜像的运行时alias。
if [ -L "$root/etc/resolv.conf" ]; then $bb rm "$root/etc/resolv.conf"; fi
[ -f "$root/etc/resolv.conf" ] || : >"$root/etc/resolv.conf"
for f in "$root"/neonvm/lib/*; do
 [ -f "$f" ] || continue
 n=${f##*/}
 if [ ! -e "$root/lib/$n" ] && [ ! -L "$root/lib/$n" ] && [ ! -e "$root/usr/lib/$n" ] && [ ! -L "$root/usr/lib/$n" ]; then
  $bb ln -s "/neonvm/lib/$n" "$root/lib/$n"
 fi
done
$bb printf '%s\n' 'SUBSYSTEM=="virtio-ports", KERNEL=="vport*", ATTR{name}=="?*", SYMLINK+="virtio-ports/$attr{name}"' >"$root/lib/udev/rules.d/99-virtio-ports.rules"
# CPU在线不通过udev任意事件写；由原Root/Host固定资源链控制。
$bb cp "$root/neonvm/bin/inittab" "$root/etc/inittab"
