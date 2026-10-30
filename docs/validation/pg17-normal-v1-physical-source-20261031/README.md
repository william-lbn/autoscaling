# PG17-U normal-v1 准确物理来源组件（2026-10-31）

13 root / 83 subcase，Go1.24.3 Linux race/count=1 实际 PASS；只包含新来源与直接受影响旧组件。实际 COPY 全包 64 个 Go 文件及本人9文件 SHA 见 JSON；原始收据 /data/neon-build/validation/p2-physical-normal-v1-20261030T172823Z，命令 nonce 收据 /data/neon-build/validation/go-1793381303553263697。

新 allowlist 采用真实 normal-v1 artifact552/resolved-config935/runtime notes3d22；input B20→resolved config 无策略变更链与 UMH/Landlock KVM 来源由独立构建收据保留。旧0ab组件保历史语义，无法把旧Origin换新profile/nonce。新RuntimeCapabilities全部实际只读，严格解码缺失/null零字段，不把默认allocator值或组件Match叫完整预算。

此U没有真实ISO/Origin捕获、预归组、PG normal实降或VM物理回归。真实新Reader KVM S由独立native收据后补，不以已有能力probe替代本代码实跑。五处拒绝不动。
