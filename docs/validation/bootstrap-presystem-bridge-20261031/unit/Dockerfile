FROM 127.0.0.1:5001/neon/pg17-dev-tools:linux-v1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build
COPY source/ .
ARG ACTUAL_NONCE
RUN --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared echo ACTUAL_BRIDGE_RUN=$ACTUAL_NONCE && mkdir -p /out && go version > /out/go-version.txt && setpriv --bounding-set=-dac_override,-dac_read_search /bin/bash -ceu 'grep CapEff /proc/thread-self/status; env GOPROXY=off GOSUMDB=off go test -mod=readonly -race -count=1 -json -timeout=120s ./pkg/dsga/resourceguard -run "^Test(Bootstrap.*PreSystem|BoundedBootstrap|SystemPlacement|PrebootstrapSystem|NormalKernelBootstrap)"' > /out/unit.json && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go test -mod=readonly -c -o /out/resourceguard-darwin.test ./pkg/dsga/resourceguard
FROM scratch
COPY --from=test /out /out
COPY --from=test /workspace /copied-source
