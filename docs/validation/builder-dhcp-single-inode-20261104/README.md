# DHCP resolver适配原只读根与单文件RW孔

真实117 Guest已获DHCP租约，但Alpine hook尝试创建`/etc/resolv.conf.$$`再rename；根目录只读且resolver是单文件RW bind，导致DNS一直未发布，PG17配置卡在Safekeeper主机名解析。此问题直接受本次内存隔离布局影响，不能用静态IP、Host DNS复制或开放整个/etc绕过。

原prepare-root.sh已存在同inode输出的awk转换，但直接从原生夹具root构建的生产路径漏执行。本修复在既有pre-base配置准备阶段一并转换确切原hook，再构建包含该字节的新base，最后Collector写任何库存前严格检查转换后字节及0755模式。未知hook、alias、硬链接或错误父目录先拒绝；原Alpine输入181f37...和转换输出8a3296...均是构建字节锁，不是runtime Source证明或固定DNS地址。测试直接执行原embedded awk逐字比较，避免双份转换漂移。旧模式/controls1不变，PG17/PG18复用同一Builder私有合同。

167bbb从准确9fec HEAD archive叠加本次5个声明文件，完整11687项源码核验通过；7根19通过事件，失败/跳过/竞态均0。唯一正式Builder原6X/help成功，实际内嵌vminit2b5与当前HEAD匹配。真实e879独立副本先拒DEV/已选base，准备和再次准备均0；仅四个路径变化（原两配置、resolver、DHCP hook），旧System/Native字节保持原件。该旧库存不代表新的合格输出；最终必须新base后重新收集并生成System/Native。

167bbb整体仍保留FAILED：最后隔离验证复制动态BusyBox时漏了同Guest musl加载器，chroot尚未执行hook。后续只修此夹具闭包、新nonce补该实际验证，禁止改旧结果或重复Go构建凑通过。真实隔离验证和独立审查分别以其后补小票为准。最终Guest、真实DHCP DNS、SQL、资格和安全内存伸缩仍未验收。大日志、旧失败和二进制保留/data，仓库不含凭据。

补充真实隔离结果：新5666e9仅补同e879 Guest的musl依赖和BusyBox applets，未改源码、未重编Builder或重跑Go测试。独立mount namespace内确认父/etc只读、resolver单文件可写；完整原hook实际重现RO创建/rename错误，转换hook的完整bound退出0，设备与inode保持相同，search及两个nameserver精确来自文档示例DHCP变量，未读Host/Pod resolver，ip是唯一固定记录mock，未配置真实网络。0.037秒PASS只证明此文件发布兼容性，真实租约与服务解析仍须新Guest启动后核验。

资源实证补充334b75：相同场景仅增加运行中自身cgroup采样，实际memory.max=3Gi、memory.swap.max=0、cpu.max=200000/100000，峰值19.8MB/OOM0，原native仍PASS。该重复不增加功能用例数量；不能用已回收transient unit的默认systemctl属性推断运行期限额。新Builder19个直接embed资产也实际从ELF读回逐字匹配冻结源码，包括当前vminit2b5与原prepare-root脚本。合成小票717bd2明确引用167原失败与各局部通过，不篡改旧终态。
