FROM 127.0.0.1:5001/neon/compute-v17-stage@sha256:4578312b21ccf702470ce42c41b46698d4eb5c02197cd83d324283b64ef0542e AS module
ARG NONCE
RUN --network=none set -eu; mkdir -p /proof; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; cat /sys/fs/cgroup/memory.max >/proof/memory.max; cat /sys/fs/cgroup/cpu.max >/proof/cpu.max; printf '%s\n' "$NONCE" >/proof/nonce; gcc --version >/proof/compiler.txt; make -j2 -s -C /postgres/src/test/modules/test_dsg_region install; sha256sum /usr/local/pgsql/bin/postgres /usr/local/pgsql/lib/test_dsg_region.so >/proof/pgstage-sha256.txt; readelf -d /usr/local/pgsql/lib/test_dsg_region.so >/proof/module-dynamic.txt
FROM 127.0.0.1:5001/neon/compute-v17@sha256:47ef74c7bee6018c1e8578486d1196bd0c9b6d84040f6ccdba97823dcd019e55 AS final
USER root
ARG NONCE
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
RUN --network=host --mount=type=cache,id=pg17-formal-array-apt-lists,target=/var/lib/apt/lists --mount=type=cache,id=pg17-formal-array-apt-cache,target=/var/cache/apt set -eu; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; export http_proxy="$HTTP_PROXY" https_proxy="$HTTPS_PROXY" no_proxy="$NO_PROXY"; apt-get update -qq; DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y python3 procps; useradd -o -u 1000 -g 1000 -M -s /usr/sbin/nologin nonroot
COPY --from=module /usr/local/pgsql/lib/test_dsg_region.so /usr/local/pgsql/lib/test_dsg_region.so
COPY --from=module /proof /test-build-proof
RUN --network=none set -eu; sha256sum /usr/local/bin/postgres /usr/local/pgsql/lib/test_dsg_region.so >/test-build-proof/runtime-sha256.txt; test "$(sha256sum /usr/local/bin/postgres | cut -d' ' -f1)" = 9f3b8eafeddcc37107e39158d1b6e30ad959bb385498db65526dc7436d2be9a7; dpkg-query -W -f='${binary:Package} ${Version}\n' python3 procps >/test-build-proof/test-tools-packages.txt
