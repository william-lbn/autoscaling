FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /w
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=512MiB CGO_ENABLED=1 GOPROXY=off
COPY . .
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir /out; sha256sum -c cohort.sha256 >/out/source-check.txt; cp cohort.sha256 /out/actual-source.sha256; set +e; go test -race -count=1 -timeout=120s -json -run '^TestNormalKernelControl' ./pkg/dsga/resourceguard >/out/test.json 2>/out/test.stderr; echo $? >/out/test.rc; CGO_ENABLED=0 go test -c -tags=dsga_native_kernel_controls -o /out/kernel-controls.test ./pkg/dsga/resourceguard >/out/native-build.log 2>&1; echo $? >/out/native-build.rc
FROM scratch
COPY --from=test /out/ /
