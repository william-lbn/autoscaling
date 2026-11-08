#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Docker daemon 只由这个普通 runner 身份访问；两个完整集成包仍属于同一总覆盖。
# root/只读来源八片保持无 Docker socket、无宿主网络，不能为了集成扩大其权限。
[[ $(uname -s) == Linux && $(id -u) != 0 ]]
[[ ${CI_TEST_SHARD_INDEX:-} == 8 ]] || { echo 'Host integration must use shard 8' >&2; exit 1; }
command -v docker >/dev/null
docker info >/dev/null
export GOMAXPROCS=2
output="$PWD/output/tests/shard-8"
mkdir -p "$(dirname "$output")"
mkdir "$output"
work=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/autoscaling-integration.XXXXXXXX")

finish() {
    status=$?
    trap - EXIT
    python3 - "$output/status.json" "$status" <<'PY'
import json, os, subprocess, sys
with open(sys.argv[1], 'w', encoding='utf-8') as output:
    json.dump({
        'exit_code': int(sys.argv[2]),
        'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'run_id': os.environ.get('GITHUB_RUN_ID'),
        'run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
        'scope': 'default-linux-go-suite-shard',
        'shard_index': 8,
        'shard_count': 9,
        'root_shard_count': 8,
        'execution_environment': 'host-integration',
        'execution_user': {'uid': os.geteuid(), 'gid': os.getegid()},
        'normal_qualification': False,
        'physical_memory_release_qualified': False,
    }, output, indent=2)
    output.write('\n')
PY
    exit "$status"
}
trap finish EXIT

# 沿原仓库固定 Kubernetes 1.31.0；开启真实 apiserver/etcd stderr，启动失败可追根因。
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@5fe7bb3edc86c7eda8c8c455a9116453ee472371
assets=$("$(go env GOPATH)/bin/setup-envtest" use 1.31.0 --bin-dir "$work/envtest" -p path)
export KUBEBUILDER_ASSETS="$assets" KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true
go mod download
export CI_TEST_OUTPUT_DIR="$output" CI_EXECUTION_ENVIRONMENT=host-integration
export CI_SOURCE_COMMIT="$(git rev-parse HEAD)"
export CI_EXECUTION_REPOSITORY="${GITHUB_REPOSITORY:-william-lbn/autoscaling}"
export CI_EXECUTION_RUN_ID="${GITHUB_RUN_ID:-0}"
export CI_EXECUTION_RUN_ATTEMPT="${GITHUB_RUN_ATTEMPT:-1}"
go version
id
docker version --format '{{.Server.Version}}'

set +e
python3 ci/test-shards.py 2>&1 | tee "$output/go-test.log"
result=("${PIPESTATUS[@]}")
set -e
[[ ${result[1]} == 0 ]] || { echo 'Failed to preserve complete integration test log' >&2; exit 1; }
exit "${result[0]}"
