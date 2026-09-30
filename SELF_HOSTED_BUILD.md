# Source-built Autoscaling images

This fork builds and publishes to `docker.io/williamluckyli`. The implementation
preserves the upstream Dockerfiles and pinned dependencies in `versions.env`.
Upstream workflows are retained in `.github/upstream-workflows/`; they are not
active because they require Neon runners, AWS roles and registries.

## Build

Configure repository Actions secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`
(a Docker Hub token with read/write permission). Push to `main`, or dispatch
**Docker Hub Autoscaling distribution**. `amd64` is the default; `arm64` is
available on native runners. Each standalone run gets a unique UTC timestamp,
source SHA, run ID and attempt in its tag.

The Neon fork calls the same workflow with an immutable Autoscaling source
commit and its distribution tag. Checkout explicitly names this repository:
reusable workflows otherwise inherit the caller repository. Artifacts are
uploaded to the calling workflow run, so Neon can publish one complete manifest.

## Published images

| Image | Source / purpose |
| --- | --- |
| autoscaling-go-base | `go-base.Dockerfile`, cached Go dependencies |
| vm-kernel | `neonvm-kernel/Dockerfile`, Linux kernel and Neon patches |
| vm-builder | `vm-builder/`, source-built VM disk builder |
| neonvm-controller | `neonvm-controller/Dockerfile` |
| neonvm-vxlan-controller | `neonvm-vxlan-controller/Dockerfile` |
| neonvm-runner | `neonvm-runner/Dockerfile`, includes our compiled kernel |
| neonvm-daemon | `neonvm-daemon/Dockerfile` |
| autoscale-scheduler | `autoscale-scheduler/Dockerfile` |
| autoscaler-agent | `autoscaler-agent/Dockerfile` |
| cluster-autoscaler-neonvm | upstream Kubernetes Autoscaler fixed commit + `ca.patch` |

Go race tests, including the upstream controller functional tests with a local
Kubernetes 1.31 API server and etcd, gate the Go builds. `ci/test.sh` provisions
these test binaries with an immutable envtest installer commit.
Images include build provenance and SBOMs.
Job outputs carry only digests; Docker Hub usernames held in Secrets would
cause GitHub to suppress full image-reference outputs. The kernel version suffix
uses the kernel directory's Git tree, so CI-only edits reuse compiled kernel layers.
All component records contain the checked-out source commit and image digest.
Images still need Kubernetes resources, permissions, configuration and KVM hosts
to run as a complete NeonVM deployment. A successful image build alone does not
validate a particular Kubernetes cluster or autoscaling policy.

Third-party OS images, package repositories, the Linux kernel archive and QEMU
firmware remain external dependencies. `versions.env` records their pinned
versions/digests; this fork does not mirror those upstream projects.
