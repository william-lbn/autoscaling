ARG UBUNTU_IMAGE
FROM ${UBUNTU_IMAGE}

# 仅供 CI 测试的本地镜像，基础由 versions.env 的架构 digest 锁定，不发布为产品。
# glibc/真实 /bin/true 用于现有 ELF 闭包测试；编译器支持 Go race/cgo，util-linux
# 提供现有 launcher/锁及只读文件系统诊断需要的工具。
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        build-essential ca-certificates git mawk python3 util-linux \
    && rm -rf /var/lib/apt/lists/*

# COPY 的真实 owner 为 root；docker run --read-only 使 fixture 到 / 的每一层
# 父目录都实际只读。inittab 必须位于生产 reader 检验的 /etc/inittab 固定路径。
COPY source-parallel/ /source-parallel/
COPY source-parallel/inittab /etc/inittab
RUN chmod 0755 /source-parallel && chmod 0444 /source-parallel/* /etc/inittab
