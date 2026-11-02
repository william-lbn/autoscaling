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
# 原Alpine DHCP hook以/etc/resolv.conf.$$+rename发布，不能用于RO /etc下的
# 单文件RW bind孔。仅本normal镜像构建期替换DNS函数的输出方式；IP/route与
# legacy镜像原样保留，DNS失败仍明确失败，绝不改Guest/Host网络配置补绕过。
dhcp="$root/neonvm/bin/udhcpc.script"
[ -f "$dhcp" ] && [ ! -L "$dhcp" ] || exit 1
patched="$dhcp.normal-prepared.$$"
trap '$bb rm -f "$patched"' 0
$bb awk '
BEGIN { suffix=" >> \"$RESOLV_CONF.$$\""; nopen=0; nappend=0; nmode=0; nclose=0; inside=0 }
$0=="resolvconf() {" { inside=1 }
{
 line=$0
 if (inside && line=="\techo -n > \"$RESOLV_CONF.$$\"") { line="\t{"; nopen++ }
 else if (inside && length(line)>=length(suffix) && substr(line,length(line)-length(suffix)+1)==suffix) { line=substr(line,1,length(line)-length(suffix)); nappend++ }
 else if (inside && line=="\tchmod a+r \"$RESOLV_CONF.$$\"") { nmode++; next }
 else if (inside && line=="\tmv -f \"$RESOLV_CONF.$$\" \"$RESOLV_CONF\"") { line="\t} > \"$RESOLV_CONF\" || exit 1"; nclose++ }
 print line
 if (inside && $0=="}") inside=0
}
END { if (nopen!=1 || nappend!=3 || nmode!=1 || nclose!=1 || inside) exit 1 }
' "$dhcp" >"$patched"
$bb sh -n "$patched"
$bb chmod 0755 "$patched"
$bb mv -f "$patched" "$dhcp"
trap - 0
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
