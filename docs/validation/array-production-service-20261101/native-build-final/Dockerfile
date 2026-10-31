FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=512MiB
COPY . .
ARG NONCE
RUN --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; mkdir -p /out; sha256sum -c actual-source-sha256.txt >/out/actual-copy-verification.txt; echo 0 >/out/source.exitcode; cp actual-source-sha256.txt /out/compiled-source-sha256.txt; set +e; go test -race -count=1 -timeout=120s -json -run '^$' ./pkg/dsga/resourceguard >/out/test.json 2>/out/test.stderr; echo $? >/out/test.exitcode; go test -count=1 -timeout=180s -json -run '^$' ./pkg/agent >/out/agent-compile.json 2>/out/agent-compile.stderr; echo $? >/out/agent-compile.exitcode; CGO_ENABLED=0 go test -c -tags=dsga_native_array_service -o /out/array-service.test ./pkg/dsga/resourceguard >/out/native-compile.stdout 2>/out/native-compile.stderr; rc=$?; echo $rc >/out/native-compile.exitcode; if [ "$rc" = 0 ]; then DSGA_ARRAY_SERVICE_EXPORT=/out/fixture /out/array-service.test -test.run=^TestExportArrayServiceFixture$ -test.v -test.timeout=20s >/out/fixture-export.log 2>&1; echo $? >/out/fixture-export.exitcode; fi
FROM scratch
COPY --from=test /out/ /
