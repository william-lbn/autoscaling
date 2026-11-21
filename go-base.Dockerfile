# Cache Go module downloads without asking go-chef to compile every dependency.
# The go-chef recipe path currently attempts to compile the stdlib internal/abi package
# as an external module package on the Linux release builder. This layer is only a
# build cache: application compilation and all functional tests remain in their
# existing downstream build/test stages.
ARG GOLANG_IMG_TAG=replaceme
ARG GOLANG_IMG_SHA=replaceme
FROM golang:$GOLANG_IMG_TAG$GOLANG_IMG_SHA AS builder

WORKDIR /workspace

# Copy only the module manifests so dependency caching is invalidated only when
# the dependency graph changes. go mod download preserves go.mod/go.sum selection.
COPY go.mod go.sum ./
RUN go mod download
