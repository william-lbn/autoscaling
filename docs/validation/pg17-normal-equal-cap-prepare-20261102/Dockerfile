FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /w
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=3 GOMEMLIMIT=1536MiB GOFLAGS=-p=2 GOPROXY=off
COPY . .
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir /out; test "$(cat /sys/fs/cgroup/memory.max)" = 3221225472; test "$(cat /sys/fs/cgroup/cpu.max)" = "300000 100000"; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; sha256sum -c .quota-core-inputs.sha256 >/out/actual-copy-check.txt; cp .quota-core-inputs.sha256 /out/compiled-source-sha256.txt; date +%s%N >/out/u.start-ns; cat /sys/fs/cgroup/cpu.stat >/out/u.start-cpu.stat; set +e; go test -p=2 -race -count=1 -timeout=180s -json -run '^(TestProductionAutomaticNormalDecreaseRequiresNegotiatedProtocol|TestProductionPrepareRetainsNormalProtocolAtEqualLimit)$' ./pkg/dsga/owner >/out/test.json 2>/out/test.stderr; echo $? >/out/u.exitcode; cat /sys/fs/cgroup/cpu.stat >/out/u.end-cpu.stat; date +%s%N >/out/u.end-ns; exit 0
FROM scratch
COPY --from=test /out/ /
