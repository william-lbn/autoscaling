#!/bin/sh
set -eu
cd /build
tar xzf /input/sources/eudev-3.2.14.tar.gz
cd eudev-3.2.14
for p in default-rules.patch load-fbcon.patch no-sgx.patch 78-sound-card.rules_Import_ID_PATH_early.patch; do patch -p1 < "/input/aports/eudev/$p"; done
patch -p1 < /input/recipe/eudev-fixed-device-v1.patch
# 只不生成manpage；原kmod和运行功能保留。新flag显式选择，legacy root不变。
CFLAGS='-O2 -fstack-protector-strong -D_FORTIFY_SOURCE=2' ./configure --sysconfdir=/etc --with-rootprefix= --with-rootrundir=/run --with-rootlibexecdir=/lib/udev --libdir=/usr/lib --enable-split-usr --disable-manpages --enable-kmod --exec-prefix=/ --bindir=/bin
make -j2
mkdir -p /out/eudev-root
make DESTDIR=/out/eudev-root sharepkgconfigdir=/usr/lib/pkgconfig install
find /out/eudev-root -type f -exec sha256sum {} + > /out/eudev-files.sha256
readelf -n -d /out/eudev-root/sbin/udevd > /out/eudev-readelf.txt
