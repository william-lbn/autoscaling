FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS build
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2
COPY . .
ARG REVIEWED_RUN_NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared  printf 'ACTUAL_REVIEWED_PHYSICAL_RUN=%s\n' "$REVIEWED_RUN_NONCE" && mkdir /out &&  find . -type f \( -name '*.go' -o -name go.mod -o -name go.sum \) -print0 | sort -z | xargs -0 sha256sum > /out/copied-source-SHA256SUMS &&  go version > /out/go-version.txt &&  go test -mod=readonly -race -count=1 -json -run '^(TestPrebootstrapSystem|TestNormalKernelBootstrap|TestBootstrapContinuesOnlyOriginalPreSystem|TestBootstrapPreSystem)' ./pkg/dsga/resourceguard | tee /out/unit.json &&  DSGA_PRESYS_EXPORT=/out/isofiles go test -mod=readonly -tags dsga_native_prebootstrap_system -count=1 -run '^TestExportPreSystemNativeFixture$' -v ./pkg/dsga/resourceguard | tee /out/exporter.log && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -mod=readonly -trimpath -c -tags dsga_native_prebootstrap_system -o /out/presys-native.test ./pkg/dsga/resourceguard &&  cd /out && sha256sum presys-native.test > binary-SHA256SUMS
FROM scratch
COPY --from=build /out/ /
