# PG17-U 内核准确构建引用组件（2026-10-31）

2 root / 6 subcase，Go1.24.3 Linux race/count=1 实际 PASS。实际 COPY 全包 59 个 Go 文件及本人两文件 SHA 见 JSON；原始收据 /data/neon-build/validation/p2-physical-kernel-build-20261030T170738Z，实际命令与 nonce 收据 /data/neon-build/validation/go-1793380058623828392。正常构建查询未支持的 normal-v1 当前必须报错，不以空 hash/旧内核 fallback。此时新 artifact 尚未锁进 Go allowlist，不能把本单元测试叫新内核启动、Origin/预归组 S、PG normal 实降或 KVM E2E。
