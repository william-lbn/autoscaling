FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS check
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=1500MiB
COPY . .
ARG NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir -p /out; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; sha256sum -c cohort.sha256 >/out/actual-copy-verification.txt; cp cohort.sha256 /out/compiled-source-sha256.txt; set +e; CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go test -c -o /out/resourceguard-darwin.test ./pkg/dsga/resourceguard >/out/compile.log 2>&1; rc=$?; echo "$rc" >/out/compile.exitcode
FROM scratch
COPY --from=check /out/ /
