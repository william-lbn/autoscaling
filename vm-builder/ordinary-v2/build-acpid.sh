#!/bin/sh
set -eu
cd /build
tar xJf /input/sources/acpid-2.0.34.tar.xz
cd acpid-2.0.34
patch -p1 < /input/aports/acpid/lfs64.patch
patch -p1 < /input/recipe/acpid-fixed-power-v1.patch
cp /input/recipe/acpid-fixed-power-client.c neon-fixed-power-client.c
CFLAGS='-O2 -fstack-protector-strong -D_FORTIFY_SOURCE=2' ./configure --prefix=/ --sysconfdir=/etc --localstatedir=/var
make -j2
mkdir -p /out/acpid-root
make DESTDIR=/out/acpid-root install
find /out/acpid-root -type f -exec sha256sum {} + > /out/acpid-files.sha256
readelf -n -d /out/acpid-root/sbin/acpid > /out/acpid-readelf.txt
