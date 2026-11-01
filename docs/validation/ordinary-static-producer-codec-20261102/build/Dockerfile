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
RUN set -eu; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; mkdir /out /build; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; sha256sum -c source.sha256 >/out/actual-source-check.txt; cp source.sha256 /out/actual-source.sha256; apk info -v >/out/actual-build-apks.txt; gcc --version >/out/compiler.txt; set +e; sh /input/revision/build-eudev.sh >/out/eudev-build.log 2>&1; echo $? >/out/test.exitcode; gcc -O2 -fstack-protector-strong -D_FORTIFY_SOURCE=2 -Wall -Wextra -Werror -Wno-unused-function -I/input/revision /input/revision/test_event_properties.c -o /out/test_event_properties >/out/codec-build.log 2>&1; codec_build=$?; echo $codec_build >/out/codec-build.exitcode; if test "$codec_build" = 0; then /out/test_event_properties >/out/codec.jsonl 2>/out/codec.stderr; echo $? >/out/codec.exitcode; fi; cat /sys/fs/cgroup/cpu.stat >/out/cpu.stat; cat /sys/fs/cgroup/memory.peak >/out/memory.peak; cat /sys/fs/cgroup/io.stat >/out/io.stat; echo 0 >/out/source.exitcode
FROM scratch
COPY --from=build /out/ /
