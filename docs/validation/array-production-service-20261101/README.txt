PG17 真实出生、四数组收费与专用请求授权服务组件（2026-11-01）

本批完成一次真实369 KVM组件链，尚未完成安全normal降限。Guest使用原256MiB出生基座、8MiB virtio-mem block，真实QMP增至1536MiB；2CPU、外层/容器2GiB上限。只读ISO、state ext4、只读/usr ext4与小initrd分离，未改宿主挂载、网络、sysctl或cgroup。

真实正向：原KernelPrep/Origin/PreSys→normal split→正式Bootstrap→真实initdb→原real UID1000/effective UID0的SUID入口→既有real-root daemon Begin生成原PIDFD/Pending/Birth并fsync→同任务exec→native base/final逐cBPF审计→迁private→released fsync→同原PIDFD注册4个真实匿名共享map_files inode→4个真实helper先触页、完整退出、完成前缀持久→PG ready→实际private AS检查→原N0完整prefix MINCORE驻留→Root只读同FD映射的8条PFN采样记录（7个不同PFN，单页数组首末同一页）均归属arrays→准确raw-body专用JWT经真实Root HTTP handler及authority WAL→PG真实Wait0。

测试边界：HostSource/issuer为TEST_ONLY夹具，运行器是同一static Go test ELF调用真实生产函数，不是已发行CLI服务主函数。S没有完整journal StartCAS→真实native Grow；该StartCAS/Grow-only/Shrink零新callback路径由直接U协议夹具覆盖。PFN是Root观察映射的PTE，不能说PM本身此前已有PTE。原4FD全部prefix先mincore驻留，若有洞后继fault新计System会owner!=A失败；不跳过缺页。没有normal memory.max下降、VM下降、故障恢复全矩阵或K8s E2E。E0，原五拒绝全部保留。

来源：固定Git2fe36基线+51精确选择，894实际COPY。9120执行73race根381子例、零fail/skip；8b3仅修改native测试探针，生产字节逐一相同，其普通-run ^$是零根ENV，不计U。最终1a0df3静态ELF只签8b3与f8811b，绝不转签旧二进制。新增25U根与原48U直接回归分开；旧5capwriter33根103子例由P2独立执行/归档，属于原a7d3db收据/888实际COPY图（生产源取849da7不可变快照，该后缀是nonce而非源码行数），不伪称最终51原图。详见catalog/source-proof/实际command、rc、COPY和testJSON。

四个共同文件guard/cancel/view/normal_split_quota_state在此图来自5fa不可变快照。Root正在独占它们的新quota/handoff版本。本清单只提交P1其余47当前源与本档案，四共同依赖单独列出；Root必须叠自身精确11源做最终joint验证并原子合入，不能覆盖其current或将旧快照冒称最新WIP。此脚本不进行git add/commit/push。

安全决定保留：新特权socket0666提案被自动审批拒绝、未执行，改为Root:可信PG主组0660及不可写父目录+原UID/GID/SO_PEERPIDFD/Pending。Setresuid(0,0,0)提案同样被拒绝、未执行、撤回；现真实daemon代办Source/WAL，SUID real UID不变。删除旧preexec-v1核心提案被拒绝、未执行；旧代码完整保留，新launch只选Begin v2且无fallback。本批没有通过包装间接重试这些拒绝。

所有失败保留：早期initramfs ENOSPC、dev/shm、loader alias、libpq/内置模块、real-root Source、private AS组及错误PM PTE断言的各原nonce、原收据、原ELF/source hash、原console不覆盖。早期Go schema/codec/race/共同依赖编译失败也保留。QMP开始前socket尚未建立的ENOENT仍在最终原收据，后继enabled与目标实读已成功；不把该瞬时状态称网络故障。

仍须后继：Begin负例/丢回复/最终drop失败的真实服务反例；Root重启冷PM原pin恢复与重建完整生命周期；真实StartCAS→签名broker→Rust原文路由→native Grow；Root quota/handoff与所有capwriter的同flock联合门禁；闭合FS/System/FI与normal持续费用，真实下降与单机全验收。不能因本组件通过提前部署K8s或启用normal资格。
