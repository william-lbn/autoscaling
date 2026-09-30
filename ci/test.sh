#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Match the upstream Makefile's Kubernetes 1.31 functional-test environment.
# Pin the installer so a future @latest toolchain change cannot break this CI.
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@5fe7bb3edc86c7eda8c8c455a9116453ee472371
export KUBEBUILDER_ASSETS
KUBEBUILDER_ASSETS="$("$(go env GOPATH)/bin/setup-envtest" use 1.31.0 --bin-dir "${RUNNER_TEMP:-$PWD/testbin}/envtest" -p path)"
find . "$KUBEBUILDER_ASSETS" -type d -exec chmod 0755 {} \;
go test -race -p 2 ./...
