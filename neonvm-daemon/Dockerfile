ARG GO_BASE_IMG=autoscaling-go-base:dev
FROM $GO_BASE_IMG AS builder

# Build the Go binary
COPY . .

# Build
RUN CGO_ENABLED=0 go build -o /neonvmd ./neonvm-daemon/cmd
# 产品程序接线；这里只定义发行构建步骤，本轮Mac不执行镜像构建。
RUN CGO_ENABLED=0 go build -o /dsga-resource-guard ./neonvm-daemon/cmd/resource-guard

FROM scratch
COPY --from=builder /neonvmd /neonvmd
COPY --from=builder /dsga-resource-guard /dsga-resource-guard
ENTRYPOINT ["/neonvmd"]
