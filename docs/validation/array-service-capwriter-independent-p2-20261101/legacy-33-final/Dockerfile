FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=1500MiB
COPY . .
ARG NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir -p /out; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; printf "%s\n" "$NONCE" >/out/nonce; sha256sum -c actual-source-sha256.txt >/out/actual-copy-verification.txt; cp actual-source-sha256.txt /out/compiled-source-sha256.txt; setpriv --bounding-set=-dac_override,-dac_read_search -- /bin/bash /workspace/run-capwriter.sh
FROM scratch
COPY --from=test /out/ /
