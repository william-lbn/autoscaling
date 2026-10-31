固定System启动来源门禁（normal-system-source-v1；组件冻结）

本批仅新增normal_system_source六源，不修改Root通道/CLI/vminit/Builder、NativePolicy、Source7、
公共record或原五层normal拒绝。旧v1行为保持。没有新kernel或整目录chown/单文件RO pin替代。

直接API
NormalSystemSourceProfileID = neon-dsga-normal-system-source-v1（与PG版本无关）。
NormalSystemSourceManifestPath = /neonvm/config/dsga-normal-system-source.json。
DecodeNormalSystemSourceManifest(raw)：1MiB/32层有界严格JSON，拒未知字段、重复key和尾随文档。
InspectNormalSystemSource(ctx,raw)：实际当前369 kernel/notes/boot、固定runtime禁项、静态对象及inittab读取。
CheckNormalSystemSource(ctx,raw)：走同一真实reader链；不接受外传Observation/bool替代事实。
NormalSystemSourceInitEntries()：返回13条固定服务副本，供新normal Builder生成。
NormalSystemSourceConfigDirectories()：返回4个固定glob配置根副本。

manifest及事实边界
manifest只要求明确版本/profile/准确369五元组、声明image摘要、compute_ctl固定path、argv/env摘要，
以及按path排序的静态文件列表（path/canonical_path/kind/bytes/sha256）。最多256对象、单个256MiB、合计512MiB。
原输入真实性不由raw参数自称trusted；DeclaredImageSHA256只是声明格式，可引用已知base image来源，
不可在内嵌manifest中循环自含最终wrapped image digest。最终image/rootdisk/原ISO绑定由外层原Source独立验证。
argv/env仅摘要，不存/打印值；实际固定argv/env执行仍由Root新入口完成，脚本hash不等于执行了固定argv。
输出有当前boot、manifest摘要、文件dev/ino/大小/hash、库存摘要；OriginalSourceAuthenticated、
ELFDependencyClosureVerified、FixedArgvEnvironmentExecuted、AllSourcesClosed、NormalDecreaseAuthorized始终false。
成功只表示此组件的真实文件/固定入口检查通过；不是原来源、完整ELF闭包、持续角色约束或normal票据。

真正落实的门禁
/neonvm/runtime 下 mounts.sh/sysctl.conf/env.sh/command.sh/args.sh/resize-swap-internal.sh 不得存在，
包括broken symlink；目录本身不得是symlink。运行时不执行/重放这些文件，reader全程零写。
实际/etc/inittab只接受编译固定13条、原顺序、无重复/缺项；可有空行/注释，拒CRLF/隐藏空白。
固定条目保留network/udev/acpid/vector/chrony/neonvmd/guard-service/vmstart/shutdown；
拒sshd、agetty、root自动登录、自定义InittabCommands、任意shell entry和裸PATH cat。
既有neonvmd managed门禁已在生产源：CPU写拒绝、文件上传仅管理TLS固定目录，未误删必要服务。

静态exec/script/config/loader对象复用Source7完整只读祖先要求，并实际解析canonical路径、
核root/regular/mode/size/hash/devino/mtime/ctime；没有放宽原Source7。
真正regular ELF对象才作为exec：只有touch/cat/sh/udhcpc/acpid可规范化到/neonvm/bin/busybox；
guard/compute_ctl等不能伪装为busybox。脚本不支持alias，完整静态脚本字节必须按原manifest匹配。
新reader独立用O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC后Fstat拒特殊文件，避免RO FIFO先阻塞；
保持完整RO父链及root/go-write检查。没有修改既有Source7打开函数。
loader条目只核文件事实，未把4字节ELF magic伪称完整metadata/DT_NEEDED/cache/RUNPATH闭包认证。

固定glob目录 /neonvm/acpi、/etc/vector、/lib/udev/rules.d、/etc/udev/rules.d 必须存在且整个祖先RO；
最多4096项/16层，实际所有文件必须已列manifest。未知额外配置、目录软链冒充文件及非regular对象拒绝。
静态vector/acpi/udev配置必须固定；动态TLS/state/run/PG扩展目录仍可另设RW孔，但不能借此获得静态代码资格。
正式none rootdisk当前RW以及extension_server要写/usr/local/lib/share/extension的事实保持：
本组件不会因此去掉RO检查、全chown或假称当前none镜像已可用。最终RO rootFS+精确RW孔由Root统一构建。

实际验证
7个精确Go race根、36子例零失败零skip；同图Darwin只编译成功（未在Mac执行）。
Git1a25125cc1d2b689803f250db08c7e2525c88271 + 仅本6源overlay；814项actual COPY哈希全匹配。
BuildKit固定Go1.24.3镜像、GOMAXPROCS2、缓存保留。客户端2GiB/CPU2不是executor硬内存上限，已准确记录。
同图native binary 19655a90c06c8e1def0748c7790930cb802133b082a35076f3594b18c5347534，
真实369 KVM（Guest1GiB/2CPU、Docker2GiB/2CPU、无网卡/无TCG）1根8子例通过，Guest/QEMU0、notes7cf精确。
子例：实际RO文件/固定inittab；runtime command拒绝；未列glob配置拒绝；RO FIFO无阻塞拒绝；
RW root祖先拒绝；即使raw manifest摘要匹配也拒root登录条目；恢复后真实reader再通过。
另新增空glob logicalRO symlink→canonical RW祖先/目标RO bind反例：
先真实核旧logical RO及空库存成立、canonical祖先确为RW，再核新gate拒绝。
本次独审修复只改新Linux/native两文件，增加canonical base完整RO父链，不改Source7。
原22路径/7子例二进制仅作为修前历史，不能认证本次修复。
这是合成静态文件fixture：exec对象由同一Go test ELF物化，只验证真实文件系统门禁/alias/字节来源；
没有启动真实System服务、compute_ctl、PG、Origin/PreSys全链，也不是正式镜像/normal/K8s/E2E认证。

Root生产消费顺序与未完成项
原NativePolicy/闭包引用版本化字段先把此固定manifest文件SHA绑定原只读ISO，不增加第9资产；
Root真实读取并核预期SHA后调用Check，不能仅拿manifest里的声明image摘要认证自身。
新normal vminit必须在旧mounts.sh/sysctl执行之前拒绝这些覆盖；失败不得fallback legacy。
BusyBox sysinit失败仍可能继续其它entry，所以vmstart/guard-service保原check-guest/原Source门禁，
不得只看ready文件。固定compute_ctl argv/env必须由原readonly JSON输入直接exec，不能沿任意shell覆盖路径。
最终要验证完整RO镜像、loader/全部配置及依赖闭包、原Host/进程/ISO生命周期和所有未来Root/System写者。
本批只提供可实际调用的严格reader/gate，所有尚未实现的角色/启动强制都保持明确false。
