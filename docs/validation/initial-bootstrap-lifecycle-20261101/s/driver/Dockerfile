FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS run
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local CGO_ENABLED=0
COPY . .
ARG TEST_NONCE
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared \
 --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared \
 set -eu; mkdir /testreceipt; printf '%s\n' "$TEST_NONCE" > /testreceipt/nonce.txt; \
 test "$(cat /sys/fs/cgroup/memory.max)" = 4294967296; test "$(cat /sys/fs/cgroup/memory.swap.max)" = 0; \
 cat /sys/fs/cgroup/cpu.max > /testreceipt/cpu.max; \
 sha256sum -c cohort-inputs.sha256 > /testreceipt/actual-source.check; cp cohort-inputs.sha256 /testreceipt/actual-source.sha256; \
 /opt/go/bin/go version > /testreceipt/go-version.txt; \
 set +e; /opt/go/bin/go test -c -tags dsga_native_initial_lifecycle -o /testreceipt/host.test ./pkg/dsga/vmguard > /testreceipt/host-compile.log 2>&1; h=$?; \
 /opt/go/bin/go test -c -tags dsga_native_initial_host_channel -o /testreceipt/guest.test ./pkg/dsga/resourceguard > /testreceipt/guest-compile.log 2>&1; g=$?; \
 printf '%s\n' "$h" > /testreceipt/host.rc; printf '%s\n' "$g" > /testreceipt/guest.rc; \
 if [ "$h" = 0 ] && [ "$g" = 0 ]; then printf '0\n' > /testreceipt/test.rc; else printf '1\n' > /testreceipt/test.rc; fi
FROM scratch
COPY --from=run /testreceipt/ /
