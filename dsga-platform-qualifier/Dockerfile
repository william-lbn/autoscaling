# 独立资格发布器：只包含静态CLI，报告/凭据由部署者以只读私有输入提供。
ARG GO_BASE_IMG=autoscaling-go-base:dev
FROM $GO_BASE_IMG AS builder
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build
ARG GO_BUILD_PARALLELISM=2
COPY . .
# 使用同一源码 go-base 已准备的依赖。空 BuildKit cache mount 会遮蔽 /go/pkg/mod，
# 在全新 GitHub runner 上导致 network=none 构建无法下载模块；不依赖本机历史缓存。
RUN --network=none CGO_ENABLED=0 go build -p=${GO_BUILD_PARALLELISM} -trimpath -ldflags="-s -w" -o /out/dsga-platform-qualifier ./dsga-platform-qualifier/cmd

FROM scratch
COPY --from=builder /out/dsga-platform-qualifier /usr/bin/dsga-platform-qualifier
# 原报告及Agent私有UDS要求root身份；不需要额外capability。
USER 0:0
ENTRYPOINT ["/usr/bin/dsga-platform-qualifier"]
