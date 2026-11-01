FS Layout2 引用一致性窄更正（2026-11-01）

原208与19640d全部保持原义/原SHA；本目录是明确的新来源、新编译、新S增量，不把旧ELF签为新行为。Root独审发现referenceLocked明确LayoutVersion2，但共用intent投影漏字段，pin/rebind两条路径落0，自洽却和公开原始引用不一致；原9U和S只覆盖Recover，漏了原FD移交路径断言。

本次仅生产normal_filesystem_origin_source_linux.go加Layout2，并把referenceLocked统一到同一投影；两旧注释16mount准确改17。普通UT增加完整投影/零版本不同断言。原生用例在真实原flock下取得reference和pin的原namespace/root FD，逐字段相同且版本2；故意零版本得到比较阶段的准确错误、新Guard无见证且JSON不变；正确原引用/两FD进入显式重绑定并恢复读回成功。错误输入返回位于Recover/任何Sync前，由实际错误点+源码顺序确认；没有冒称真实硬件sync故障注入或逐syscall跟踪。

准确Git955ad6477ba89ade35ffaf73828d53d44a0bb3b8+独占10源，没有拍入Root/P2活动WIP。新570b76实核949COPY，BuildKit和外层均2CPU/2GiB；10race根39子例、无fail/skip，其中旧9根直接回归、新1根PG17-U-FS-ORIGIN-010（源码短注释PG17-U-FS-010是该稳定ID别名）。KernelPrep/PreSys逻辑本次未变，不重复其原16根，不转签旧25/117。新ELF496dc476...实际369/notes7cf...独立640aa7 Guest，7.52s、Guest0/唯一QEMU Wait0，准确same-ID PG17-S-FILESYSTEM-ORIGIN-001复测，不算新增第二S。

原全部关键行为也在新Guest同次执行：PID1真实switch_root/normal vminit/单ext417mount与LSMsecurityfs、原8WAL、Split→Bootstrap→CheckGuest、三隐藏RW持有拒且零FSWAL、privateCOW、原10WALsync、其它活task下Observe、UID1000 PGDATA重建/warmup和停机后只读ext4 payload。Host challenge为DEV fixture，System未运行程序为staticstand-in，无PG/新Root broker/持续normal/K8s/E；资格始终Unknown。这里只是同Root新Guard重绑定，不自称跨Root broker exec证明。

联合stage=旧208全部历史+准确当前10（仅3路径覆盖）+本新档案。旧manifest中的旧10路径hash是历史actual-source说明；本目录新joint manifest以当前10为准，并将原源副本映射保存。保留原首次失败/所有old日志；没有Git操作，Root统一独审/提交。
