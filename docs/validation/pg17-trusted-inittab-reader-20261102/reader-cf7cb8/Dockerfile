# syntax=docker/dockerfile:1-labs
FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS build
USER root
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=768MiB GOFLAGS=-p=1 GOPROXY=off
WORKDIR /work
COPY . .
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir /out; test "$(cat /sys/fs/cgroup/memory.max)" = 3221225472; test "$(cat /sys/fs/cgroup/cpu.max)" = "300000 100000"; sha256sum -c .trusted-inputs.sha256 >/out/actual-copy-verification.txt; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; cat /sys/fs/cgroup/memory.max >/out/memory.max; printenv GOMAXPROCS GOMEMLIMIT GOFLAGS >/out/go-env.txt; go version >/out/go-version.txt; set +e; go test -p=1 -race -json -count=1 -timeout=90s -run '^TestNormalSystemSourceTrustedInittabDeployment$' ./pkg/dsga/resourceguard >/out/test.json 2>/out/test.stderr; echo $? >/out/u.exitcode; CGO_ENABLED=0 go build -p=1 -o /out/resource-guard.real ./neonvm-daemon/cmd/resource-guard >/out/cli.log 2>&1; echo $? >/out/cli.exitcode; CGO_ENABLED=0 go test -p=1 -c -ldflags="-s -w" -tags=dsga_native_trusted_control,dsga_native_stock_preparation,dsga_native_ordinary2,dsga_native_role_transfer2,dsga_native_role_service,dsga_native_system_lifecycle -o /out/trusted-guest.test ./pkg/dsga/resourceguard >/out/guest.log 2>&1; echo $? >/out/guest.exitcode; cat /sys/fs/cgroup/cpu.stat >/out/cpu.stat; cat /sys/fs/cgroup/memory.peak >/out/memory.peak; cat /sys/fs/cgroup/io.stat >/out/io.stat; exit 0
FROM scratch
COPY --from=build /out/ /
