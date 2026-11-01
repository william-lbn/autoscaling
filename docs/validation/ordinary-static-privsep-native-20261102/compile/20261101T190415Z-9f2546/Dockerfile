ARG TOOLS_BASE
FROM ${TOOLS_BASE} AS build
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ARG http_proxy
ARG https_proxy
ARG no_proxy
RUN apk add --no-cache gcc=13.2.1_git20231014-r0 g++=13.2.1_git20231014-r0 musl-dev=1.2.4_git20230717-r6 make=4.4.1-r2 pkgconf=2.1.0-r0 gperf=3.1-r4 glib-dev=2.78.6-r0 linux-headers=6.5-r0 kmod-dev=31-r2 util-linux-dev=2.39.3-r0 libtool=2.4.7-r3 autoconf=2.71-r2 automake=1.16.5-r2 binutils=2.41-r1 patch=2.7.6-r10
WORKDIR /input
COPY . .
ARG NONCE
RUN --network=none set -eu; test "$(cat /sys/fs/cgroup/memory.max)" = 3221225472; test "$(cat /sys/fs/cgroup/cpu.max)" = "200000 100000"; mkdir /out; trap 'rc=$?; for m in cpu.stat memory.peak io.stat memory.events; do cat /sys/fs/cgroup/$m >/out/$m; done; exit "$rc"' EXIT; gcc --version >/out/compiler.txt; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; sha256sum -c source.sha256 >/out/actual-source-check.txt; cp source.sha256 /out/actual-source.sha256; cp stock/libudev.so.1 /out/stock-libudev.so.1; gcc -O2 -c stock/MurmurHash2.c -o /out/MurmurHash2.o; set +e; gcc -O2 -Wall -Wextra -Werror -Wno-unused-function -Istock -Irevision -o /out/static-privsep-native revision/test_static_privsep_native.c /out/MurmurHash2.o -Lstock -Wl,-rpath-link,/input/stock -l:libudev.so.1 >/out/compile.stdout 2>/out/compile.stderr; rc=$?; echo "$rc" >/out/compile.exitcode; if test "$rc" = 0; then readelf -n -d /out/static-privsep-native >/out/readelf.txt; fi; exit "$rc"
FROM scratch
COPY --from=build /out/ /
