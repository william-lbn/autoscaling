FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=1500MiB
COPY . .
ARG NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir -p /out; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; test $(cat /out/memory.max) = 2147483648; sha256sum -c actual-source-sha256.txt >/out/actual-copy-verification.txt; cp actual-source-sha256.txt /out/compiled-source-sha256.txt; go version >/out/go-version.txt; printf '%s\n' "$NONCE" >/out/nonce; set +e; ( go test -race -count=1 -json ./vm-builder && go test -race -count=1 -json -run '^TestNormalSystemSource' ./pkg/dsga/resourceguard && CGO_ENABLED=0 go build -o /out/vm-builder ./vm-builder && /out/vm-builder -version >/out/version.txt && CGO_ENABLED=0 go test -c -tags=dsga_native_normal_system_source -o /out/normal-system-source.test ./pkg/dsga/resourceguard && DSGA_NATIVE_SYSTEM_FIXTURE_OUTPUT=/out/system-fixture /out/normal-system-source.test -test.run '^TestNativeNormalSystemV2Fixture$' -test.v -test.count=1 >/out/fixture-build.log && for f in vm-builder/files/normal-split/*; do /bin/sh -n $f || exit; done ) >/out/test.json 2>/out/test.stderr; echo $? >/out/test.exitcode
FROM scratch
COPY --from=test /out/ /
