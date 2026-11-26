ARG GO_BASE_IMG=autoscaling-go-base:dev
ARG ALPINE_IMG_TAG=replaceme
ARG ALPINE_IMG_SHA=replaceme
FROM $GO_BASE_IMG AS builder

COPY . .
# The base stage prefetches the exact go.mod/go.sum module graph. This build compiles
# the scheduler and its dependencies from source; keep CGO_ENABLED consistent with the
# other release binaries so the resulting image remains statically linked.
RUN CGO_ENABLED=0 go build -o main ./autoscale-scheduler/cmd

FROM alpine:$ALPINE_IMG_TAG$ALPINE_IMG_SHA
COPY --from=builder /workspace/main /usr/bin/kube-scheduler
ENTRYPOINT ["/usr/bin/kube-scheduler"]
