FROM 127.0.0.1:5001/neon/pg17-dev-tools:linux-v1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build
COPY source/ .
ARG ACTUAL_NONCE
RUN --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared mkdir -p /private/tmp && chmod 0700 /private/tmp && echo ACTUAL_QMP_GEOMETRY_RUN=$ACTUAL_NONCE && CGO_ENABLED=0 go test -c -tags dsga_native_qmp_geometry -o /out/qmp-geometry.test ./pkg/dsga/vmguard
FROM scratch AS export
COPY --from=test /out/qmp-geometry.test /qmp-geometry.test
COPY --from=test /workspace /copied-source
