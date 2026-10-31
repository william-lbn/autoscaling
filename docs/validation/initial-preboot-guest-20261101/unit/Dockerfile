FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2
COPY . .
ARG NONCE
RUN --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir -p /out; sha256sum -c actual-source-sha256.txt >/out/actual-copy-verification.txt; echo 0 >/out/source.exitcode; cp actual-source-sha256.txt /out/compiled-source-sha256.txt; printf '%s\n' "$NONCE" >/out/nonce; set +e; ( go test -race -count=1 -timeout=120s -json -run '^(TestInitialPrebootGuest|TestRuntimeBootRequirements)' ./pkg/dsga/resourceguard && CGO_ENABLED=0 go test -c -tags=dsga_native_initial_preboot_guest -o /out/initial-guest.test ./pkg/dsga/resourceguard && DSGA_INITIAL_GUEST_EXPORT=/out/isofiles /out/initial-guest.test -test.run '^TestExportInitialPrebootGuestFixture$' -test.v >/out/fixture-export.log 2>&1 ) >/out/test.json 2>/out/test.stderr; echo $? >/out/test.exitcode
FROM scratch
COPY --from=test /out/ /
