FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=512MiB
COPY . .
ARG NONCE
RUN --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; mkdir -p /out; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; sha256sum -c actual-source-sha256.txt >/out/actual-copy-verification.txt; echo 0 >/out/source.exitcode; cp actual-source-sha256.txt /out/compiled-source-sha256.txt; set +e; echo 0 >/out/test.exitcode; CGO_ENABLED=0 go test -c -tags=dsga_native_array_service,dsga_native_pg17_array_resize -o /out/array-resize.test ./pkg/dsga/resourceguard >/out/native-compile.stdout 2>/out/native-compile.stderr; rc=$?; echo $rc >/out/native-compile.exitcode; if [ "$rc" = 0 ]; then DSGA_ARRAY_SERVICE_EXPORT=/out/fixture /out/array-resize.test -test.run=^TestExportArrayServiceFixture$ -test.v -test.timeout=30s >/out/fixture-export.log 2>&1; echo $? >/out/fixture-export.exitcode; fi
FROM scratch
COPY --from=test /out/ /
