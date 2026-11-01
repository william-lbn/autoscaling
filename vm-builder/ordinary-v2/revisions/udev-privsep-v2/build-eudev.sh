#!/bin/sh
set -eu
cd /build
tar xzf /input/sources/eudev-3.2.14.tar.gz
cd eudev-3.2.14
for p in default-rules.patch load-fbcon.patch no-sgx.patch 78-sound-card.rules_Import_ID_PATH_early.patch; do patch -p1 < "/input/aports/eudev/$p"; done
patch -p1 < /input/frozen/eudev-fixed-device-v1.patch
patch -p1 < /input/revision/eudev-privsep-v2.patch
cp /input/revision/protocol.h src/udev/protocol.h
cp /input/revision/creator-ipc.c src/udev/neon-creator-ipc.c
cp /input/revision/emitter-client.c src/udev/neon-emitter-client.c
# 只有udevd新fixed分支变；公共libudev源码逐字与原archive相同。
sha256sum src/libudev/libudev-monitor.c >/out/public-libudev-source.sha256
test "$(sha256sum src/libudev/libudev-monitor.c | cut -d' ' -f1)" = e5cbc5e1e50a1e1635eea8e91c4cc3690034e9a5ed1673597caa8a3f5da1a461
CFLAGS='-O2 -fstack-protector-strong -D_FORTIFY_SOURCE=2' ./configure --sysconfdir=/etc --with-rootprefix= --with-rootrundir=/run --with-rootlibexecdir=/lib/udev --libdir=/usr/lib --enable-split-usr --disable-manpages --enable-kmod --exec-prefix=/ --bindir=/bin
make -j2
mkdir -p /out/eudev-root
make DESTDIR=/out/eudev-root sharepkgconfigdir=/usr/lib/pkgconfig install
readelf -n -d /out/eudev-root/sbin/udevd >/out/eudev-readelf.txt
