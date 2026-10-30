FROM 127.0.0.1:5001/neon/pg17-dev-tools:linux-v1
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build
COPY . .
ARG DEV_COMMAND
ARG DEV_RUN_NONCE
RUN --mount=type=cache,id=autoscaling-gomod-amd64,target=/go/pkg/mod,sharing=shared \
    --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared \
    printf 'ACTUAL_LINUX_TEST_RUN=%s\n' "$DEV_RUN_NONCE" && /bin/bash -c "$DEV_COMMAND"
