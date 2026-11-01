FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=1500MiB
COPY . .
ARG NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir -p /out; test "$(cat /sys/fs/cgroup/memory.max)" = 2147483648; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; printf "%s\n" "$NONCE" >/out/nonce; sha256sum -c cohort.sha256 >/out/actual-copy-verification.txt; cp cohort.sha256 /out/compiled-source-sha256.txt; gcc-12 -nostdlib -static -fno-stack-protector -fno-pie -no-pie -O2 -Wl,--build-id=none -o /out/system-role system-role-fixture.c; readelf -l /out/system-role >/out/system-role-readelf.txt; set +e; ( set -e; go test -race -count=1 -timeout=120s -json -run "^Test(NormalSystemRole|NormalSystemLifecycle|ArrayRuntimeClosure)" ./pkg/dsga/resourceguard >/out/test.json 2>/out/test.stderr; CGO_ENABLED=0 go test -c -tags=dsga_native_system_role,dsga_native_system_lifecycle -o /out/lifecycle.test ./pkg/dsga/resourceguard; DSGA_SYSTEM_ROLE_ELF=/out/system-role DSGA_SYSTEM_LIFECYCLE_EXPORT=/out/fixture /out/lifecycle.test -test.run "^TestExportNormalSystemRoleFixture$" -test.v >/out/fixture-export.log 2>&1 ); echo $? >/out/test.exitcode
FROM scratch
COPY --from=test /out/ /
