FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /w
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=512MiB CGO_ENABLED=1 GOPROXY=off
COPY . .
RUN --network=none --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir /out; cat /sys/fs/cgroup/memory.max >/out/memory.max; cat /sys/fs/cgroup/cpu.max >/out/cpu.max; test "$(cat /out/memory.max)" = 2147483648; go version >/out/go-version.txt; sha256sum -c .signal-cohort.sha256 >/out/source-check.txt; cp .signal-cohort.sha256 /out/actual-source.sha256; set +e; go test -race -count=1 -timeout=120s -json -run '^TestNormalKernel(Control|Signal)' ./pkg/dsga/resourceguard >/out/test.json 2>/out/test.stderr; echo $? >/out/test.rc; CGO_ENABLED=0 go test -c -tags=dsga_native_kernel_controls,dsga_native_kernel_control_signal -o /out/kernel-controls.test ./pkg/dsga/resourceguard >/out/native-build.log 2>&1; rc=$?; echo $rc >/out/native-build.rc; if test $rc = 0; then go tool objdump -s 'normalKernelSignalScopeExecRaw' /out/kernel-controls.test >/out/terminal-asm.txt; fi
FROM scratch
COPY --from=test /out/ /
