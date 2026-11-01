FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=512MiB CGO_ENABLED=1 GOPROXY=off
COPY . .
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir /out; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; test "$(cat /out/memory.max)" = 2147483648; go version >/out/go-version.txt; sha256sum -c .metadata-cohort.sha256 >/out/source-check.txt; cp .metadata-cohort.sha256 /out/actual-source.sha256; set +e; go test -race -count=1 -timeout=150s -json -run "^(TestMetadataWiring|TestKernelPrepV2Handoff)" ./pkg/dsga/resourceguard ./pkg/dsga/vmguard ./pkg/dsga/bootprotocol >/out/test.json 2>/out/test.stderr; echo $? >/out/test.rc
FROM scratch
COPY --from=test /out/ /
