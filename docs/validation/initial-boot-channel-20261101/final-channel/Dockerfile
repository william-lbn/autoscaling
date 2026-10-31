FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOMAXPROCS=2 GOMEMLIMIT=512MiB GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build
COPY source/ ./
ARG NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir -p /out; sha256sum -c cohort.sha256 > /out/source-check.txt; echo 0 > /out/source-check.rc; cp cohort.sha256 /out/actual-source.sha256; printf '%s\n' "$NONCE" > /out/nonce.txt; go version > /out/go-version.txt; set +e; go test -p 2 -race -count=1 -json -run '^(TestInitialBootTransport|TestInitialPreboot|TestInitialMovable)' ./pkg/dsga/bootprotocol ./pkg/dsga/vmguard > /out/test.json 2>/out/test.stderr; echo $? > /out/test.rc
FROM scratch
COPY --from=test /out/ /
