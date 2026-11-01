FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS common
WORKDIR /w
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=512MiB GOPROXY=off
COPY . .
FROM common AS test
ENV CGO_ENABLED=1
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir /out; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; test "$(cat /out/memory.max)" = 2147483648; sha256sum -c cohort.sha256 >/out/source-check.txt; cp cohort.sha256 /out/actual-source.sha256; set +e; go test -race -count=1 -timeout=600s -json -run '^Test(Initial|NormalMemory|OriginalMemoryGeometry)' ./pkg/dsga/bootprotocol ./pkg/dsga/resourceguard ./pkg/dsga/vmguard >/out/test.json 2>/out/test.stderr; echo $? >/out/test.rc;
FROM scratch AS testreceipt
COPY --from=test /out/ /
FROM common AS native
ENV CGO_ENABLED=0
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir /out; sha256sum -c cohort.sha256 >/out/source-check.txt; cp cohort.sha256 /out/actual-source.sha256; set +e; go test -c -tags=dsga_native_initial_lifecycle,dsga_native_memory_origin_wire -o /out/host.test ./pkg/dsga/vmguard >/out/host-build.log 2>&1; echo $? >/out/host-build.rc; go test -c -tags=dsga_native_memory_origin_wire -o /out/guest.test ./pkg/dsga/resourceguard >/out/guest-build.log 2>&1; echo $? >/out/guest-build.rc;
FROM scratch AS nativereceipt
COPY --from=native /out/ /
