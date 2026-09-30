# syntax=docker/dockerfile:1.7
ARG GO_BASE_IMG
ARG ALPINE_IMG_TAG
ARG ALPINE_IMG_SHA
FROM ${GO_BASE_IMG} AS builder
ARG VERSION
ARG DAEMON_IMAGE
COPY . .
RUN set -eu; . ./versions.env; \
    CGO_ENABLED=0 go build -trimpath -o /vm-builder \
      -ldflags "-s -w -X main.Version=$VERSION -X main.NeonvmDaemonImage=$DAEMON_IMAGE \
      -X main.AlpineImageTag=$ALPINE_IMG_TAG -X main.AlpineImageShaAmd64=$ALPINE_IMG_SHA_AMD64 \
      -X main.AlpineImageShaArm64=$ALPINE_IMG_SHA_ARM64 -X main.BusyboxImageTag=$BUSYBOX_IMG_TAG \
      -X main.BusyboxImageShaAmd64=$BUSYBOX_IMG_SHA_AMD64 -X main.BusyboxImageShaArm64=$BUSYBOX_IMG_SHA_ARM64" ./vm-builder
FROM alpine:${ALPINE_IMG_TAG}${ALPINE_IMG_SHA}
RUN apk add --no-cache ca-certificates
COPY --from=builder /vm-builder /usr/local/bin/vm-builder
ENTRYPOINT ["/usr/local/bin/vm-builder"]
