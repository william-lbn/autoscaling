FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS run
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local
COPY . .
ARG TEST_NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared \
 --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared \
 set -eu; mkdir -p /private/tmp; chmod 1777 /private/tmp; mkdir /testreceipt; printf '%s\n' "$TEST_NONCE" > /testreceipt/nonce.txt; \
 test "$(cat /sys/fs/cgroup/memory.max)" = 4294967296; test "$(cat /sys/fs/cgroup/memory.swap.max)" = 0; \
 cat /sys/fs/cgroup/cpu.max > /testreceipt/cpu.max; \
 sha256sum -c cohort-inputs.sha256 > /testreceipt/actual-source.check; cp cohort-inputs.sha256 /testreceipt/actual-source.sha256; \
 /opt/go/bin/go version > /testreceipt/go-version.txt; \
 set +e; CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 /opt/go/bin/go test -c -o /testreceipt/vmguard-darwin.test ./pkg/dsga/vmguard > /testreceipt/test.jsonl 2> /testreceipt/test.stderr; rc=$?; set -e; printf '%s\n' "$rc" > /testreceipt/test.rc
FROM scratch
COPY --from=run /testreceipt/ /
