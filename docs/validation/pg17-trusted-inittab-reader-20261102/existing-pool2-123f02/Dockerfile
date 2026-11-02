FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS check
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=4 GOMEMLIMIT=1536MiB GOFLAGS=-p=2 GOPROXY=off
COPY . .
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir /out; test "$(cat /sys/fs/cgroup/memory.max)" = 4294967296; test "$(cat /sys/fs/cgroup/cpu.max)" = "400000 100000"; sha256sum -c .parallel-inputs.sha256 >/out/actual-copy-verification.txt; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; cat /sys/fs/cgroup/memory.max >/out/memory.max; go version >/out/go-version.txt; set +e; CGO_ENABLED=1 go test -race -c -o /out/source-parallel-race.test ./pkg/dsga/resourceguard >/out/compile.log 2>&1; rc=$?; echo $rc >/out/compile.exitcode; cat /sys/fs/cgroup/cpu.stat >/out/cpu.stat; cat /sys/fs/cgroup/memory.peak >/out/memory.peak; exit 0
FROM scratch
COPY --from=check /out/ /
