PG17 数组 helper 原内核 profile 接线修复

稳定编号：PG17-U-ARRAY-ORIGINAL-KERNEL-PROFILE-1；PG17-B-ARRAY-PROFILE-CLI-GUEST-1。
生产唯一调用点使用其持有的原 native 来源，ISO/封存来源一致且 VM/Compute 绑定后，通过既有 DeriveRuntimeBootRequirements 选择固定 metadata2/KernelPrep2 构建；原 standalone 仍固定369。实际 kernel release/notes、帧、硬限及 helper AS 不变。新增诊断仅输出固定阶段和 errno，保持原错误链。
48b3c9 实际4个race根35子项、0跳过通过，CLI/Guest两构建通过，56.173秒。8172首次编译失败原记录保留。Root 独立审查修复完整差异；真实 normal/VM 验收由后续原nonce完整运行单独判断，本批不签 Source、normal 或 E/K8s PASS。大产物留 /data/neon-build/validation。
