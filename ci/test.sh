#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Linux root 分片包含真实 root-owned/WAL/pidfd/只读来源检查。宿主 runner 的
# UID1001 或可写根都不满足合同；在一次性隔离容器内提供真实条件，不改生产准入。
[[ $(uname -s) == Linux ]] || { echo 'CI requires native Linux and Docker' >&2; exit 1; }
[[ ${CI_TEST_SHARD_INDEX:-} =~ ^[0-7]$ ]] || { echo 'Set CI_TEST_SHARD_INDEX to 0..7; all eight shards are required' >&2; exit 1; }
command -v docker >/dev/null
docker info >/dev/null
export GOMAXPROCS=2
output="$PWD/output/tests/shard-$CI_TEST_SHARD_INDEX"
mkdir -p "$(dirname "$output")"
# 证据目录必须全新；本地复跑也不得把历史 Go JSON 追加成“本次”成功记录。
mkdir "$output"
work=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/autoscaling-tests.XXXXXXXX")
host_uid=$(id -u)
host_gid=$(id -g)
gocache=$(go env GOCACHE)
modcache=$(go env GOMODCACHE)
goroot=$(go env GOROOT)
mkdir -p "$gocache" "$modcache" "$work/context" "$work/tmp"
container="autoscaling-tests-$(basename "$work")"
image_id=''

# root 只写这两个 Go 专用缓存。无论测试成功或失败，都恢复宿主归属，使显式
# always cache/save 和下一次安装/编译可读写；不 chmod 模块源码，也不改 runner 其他目录。
finish() {
    status=$?
    trap - EXIT
    docker rm -f "$container" >/dev/null 2>&1 || true
    if [[ -n "$image_id" ]]; then
        if ! docker run --rm --network=none --read-only --user=0:0 \
            --cap-drop=ALL --cap-add=CHOWN --cap-add=DAC_OVERRIDE --cap-add=FOWNER \
            --security-opt=no-new-privileges \
            --mount "type=bind,src=$gocache,dst=/go-cache" \
            --mount "type=bind,src=$modcache,dst=/go-modcache" \
            --entrypoint /bin/chown "$image_id" -R --no-dereference \
            "$host_uid:$host_gid" /go-cache /go-modcache; then
            echo 'Failed to return Go cache ownership to the runner' >&2
            status=1
        fi
    fi
    python3 - "$output/status.json" "$status" "$image_id" <<'PY'
import json, os, subprocess, sys
with open(sys.argv[1], 'w', encoding='utf-8') as output:
    json.dump({
        'exit_code': int(sys.argv[2]),
        'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'test_image_id': sys.argv[3],
        'run_id': os.environ.get('GITHUB_RUN_ID'),
        'run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
        'scope': 'default-linux-go-suite-shard',
        'shard_index': int(os.environ['CI_TEST_SHARD_INDEX']),
        'shard_count': 9,
        'root_shard_count': 8,
        'execution_environment': 'root-readonly',
        'normal_qualification': False,
        'physical_memory_release_qualified': False,
    }, output, indent=2)
    output.write('\n')
PY
    exit "$status"
}
trap finish EXIT

# Match the upstream Makefile's Kubernetes 1.31 functional-test environment.
# Pin the installer so a future @latest toolchain change cannot break this CI.
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@5fe7bb3edc86c7eda8c8c455a9116453ee472371
assets=$("$(go env GOPATH)/bin/setup-envtest" use 1.31.0 --bin-dir "$work/envtest" -p path)
# 安装器缓存路径可能不可被其他 uid 遍历；只规范实际 envtest 工具目录。
find "$work/envtest" -type d -exec chmod 0755 {} +
go mod download
go run -mod=readonly ./ci/test-fixtures "$work/context/source-parallel"
cp "$work/context/source-parallel/manifest.json" "$output/source-parallel-manifest.json"

# 镜像沿用仓库已锁定的原生架构 Ubuntu digest，不消费滚动 tag。
# shellcheck source=../versions.env
source versions.env
case "$(go env GOARCH)" in
    amd64) ubuntu_sha=$UBUNTU_IMG_SHA_AMD64 ;;
    arm64) ubuntu_sha=$UBUNTU_IMG_SHA_ARM64 ;;
    *) echo 'Unsupported native test architecture' >&2; exit 1 ;;
esac
# Optional self-hosted runner egress: a local HTTP proxy on the host requires
# host networking for this build step only. Proxy values use Docker's predefined
# proxy build args (not persisted in image history); the test container remains --network=none.
docker_build_args=()
if [[ -n "${CI_TEST_DOCKER_BUILD_NETWORK:-}" ]]; then
    docker_build_args+=(--network "$CI_TEST_DOCKER_BUILD_NETWORK")
    for proxy in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
        value="${!proxy:-}"
        if [[ -n "$value" ]]; then docker_build_args+=(--build-arg "$proxy=$value"); fi
    done
fi
docker build "${docker_build_args[@]}" --build-arg "UBUNTU_IMAGE=ubuntu:${UBUNTU_IMG_TAG}${ubuntu_sha}" \
    --iidfile "$work/image-id" --file ci/test.Dockerfile "$work/context" \
    2>&1 | tee "$output/image-build.log"
image_id=$(cat "$work/image-id")

# 根及静态 fixture 都来自只读镜像；只有临时文件和 Go 缓存可写。CHOWN 用于真实
# owner 正/反例，DAC_OVERRIDE/FOWNER 用于对 0444/0555 fixture 注入损坏后检验拒绝。
# resourceguard 使用真实 F_SETPIPE_SZ 将 PostgreSQL stderr pipe 设为 64 KiB；
# 在宿主 pipe-user-pages 配额下需要 SYS_RESOURCE。仅额外授予这一项，不授 SYS_ADMIN/SYS_PTRACE，
# 不开放宿主 pid/ns、Docker socket 或 secrets；容器仍只读根、无网络且受 CPU/内存/PID 上限约束。
# 依赖已在宿主下载，容器仅保留 loopback 给 envtest/HTTP/UDS，不访问外网。
set +e
docker run --rm --name "$container" --network=none --read-only --user=0:0 \
    --cap-drop=ALL --cap-add=CHOWN --cap-add=DAC_OVERRIDE --cap-add=FOWNER --cap-add=SYS_RESOURCE \
    --security-opt=no-new-privileges --cpus=2 --memory=6g --memory-swap=6g --pids-limit=1024 \
    --tmpfs /root:rw,nodev,nosuid,mode=0700,size=16m \
    --mount "type=bind,src=$PWD,dst=/workspace,readonly" \
    --mount "type=bind,src=$goroot,dst=/opt/go,readonly" \
    --mount "type=bind,src=$assets,dst=/envtest,readonly" \
    --mount "type=bind,src=$work/tmp,dst=/tmp" \
    --mount "type=bind,src=$gocache,dst=/go-cache" \
    --mount "type=bind,src=$modcache,dst=/go-modcache" \
    --mount "type=bind,src=$output,dst=/test-results" \
    --env "CI_TEST_SHARD_INDEX=$CI_TEST_SHARD_INDEX" \
    --env "CI_SOURCE_COMMIT=$(git rev-parse HEAD)" \
    --env "CI_EXECUTION_REPOSITORY=${GITHUB_REPOSITORY:-william-lbn/autoscaling}" \
    --env "CI_EXECUTION_RUN_ID=${GITHUB_RUN_ID:-0}" \
    --env "CI_EXECUTION_RUN_ATTEMPT=${GITHUB_RUN_ATTEMPT:-1}" \
    --workdir /workspace --entrypoint /bin/bash "$image_id" /workspace/ci/test-container.sh \
    2>&1 | tee "$output/go-test.log"
result=("${PIPESTATUS[@]}")
set -e
[[ ${result[1]} == 0 ]] || { echo 'Failed to preserve complete test log' >&2; exit 1; }
exit "${result[0]}"
