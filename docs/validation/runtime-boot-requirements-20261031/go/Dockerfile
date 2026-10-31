FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS build
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOPROXY=off GOSUMDB=off
COPY source/ .
ARG NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared \
 set +e; mkdir /out; printf '%s\n' "$NONCE" > /out/nonce.txt; go version > /out/go-version.txt; \
 sha256sum -c cohort.sha256 > /out/source-check.txt 2>&1; src=$?; echo "$src" > /out/source-check.rc; \
 find . -type f \( -name '*.go' -o -name go.mod -o -name go.sum \) -print0 | sort -z | xargs -0 sha256sum > /out/actual-source.sha256; \
 go test -mod=readonly -race -p=2 -count=1 -timeout=90s -json -run '^(TestRuntimeBootRequirements|TestRuntimePolicyTemplates)' ./pkg/dsga/resourceguard > /out/unit.json 2> /out/unit.stderr; echo "$?" > /out/unit.rc; \
 CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -mod=readonly -p=2 ./pkg/dsga/resourceguard > /out/darwin-build.stdout 2> /out/darwin-build.stderr; echo "$?" > /out/darwin-build.rc; true
FROM scratch
COPY --from=build /out/ /
