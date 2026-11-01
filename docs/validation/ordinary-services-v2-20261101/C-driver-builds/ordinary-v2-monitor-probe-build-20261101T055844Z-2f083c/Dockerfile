FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS build
WORKDIR /input
COPY . .
ARG NONCE
RUN set -eu; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; mkdir /out; gcc --version >/out/compiler.txt; cp *.c /out/; set +e; gcc -static -O2 -Wall -Wextra -Werror -o /out/ordinary-monitor test_udev_monitor.c >/out/compile.stdout 2>/out/compile.stderr; echo $? >/out/test.exitcode; echo 0 >/out/source.exitcode
FROM scratch
COPY --from=build /out/ /
