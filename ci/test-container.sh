#!/usr/bin/env bash
set -euo pipefail
umask 022
[[ $(id -u) == 0 && $(id -g) == 0 ]]

# bind mount 的目录最初由宿主 runner 建立；测试内 /tmp 仍应具备真实 root/1777
# 语义。仅改这次运行私有目录，绝不把可写宿主根当只读来源。
chown 0:0 /tmp
chmod 1777 /tmp
mkdir -p /tmp/gopath
export GOPATH=/tmp/gopath GOROOT=/opt/go
export PATH=/opt/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export GOCACHE=/go-cache GOMODCACHE=/go-modcache GOMAXPROCS=2
export KUBEBUILDER_ASSETS=/envtest GOTOOLCHAIN=local GOPROXY=off
# 源码以 runner uid 的只读 bind mount 提供，只把本次唯一 checkout 标为可信。
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0=/workspace
printf 'CI scope: complete default Linux Go tests; no normal/physical qualification\n'
id
uname -a
go version
sed -n '/^Uid:/p;/^Gid:/p;/^Cap/p;/^NoNewPrivs:/p;/^Seccomp:/p' /proc/self/status
findmnt -n -o TARGET,OPTIONS -T /source-parallel

# driver 实时枚举全部默认 Test/Example/Fuzz，把 owner/journal 顶层按固定 hash
# 分到八片；其余包仅在第零片完整执行。独立汇总 job 在镜像构建前证明全覆盖。
exec python3 /workspace/ci/test-shards.py
