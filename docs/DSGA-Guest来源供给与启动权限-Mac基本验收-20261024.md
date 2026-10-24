# Guest原来源供给与启动权限

本批把资源执行器接入产品启动源码；只编写/编译程序和校验模板，**没有构建镜像、
安装SUID、生成部署或执行K8s**。默认仍关闭。控制面同一immutable Secret的
`resource.json`生产者及Neon镜像补充脚本会在依赖先推送后继续接线。

## 来源与真实启动流程

`SourceConfig`固定原VM/Compute、状态目录、初始epoch1/预算引用、完整显式Limits
及管理公钥，拒绝猜比例/RSS默认值和私钥。`RuntimeDocument`保留原Secret UID、
incarnation、resource.json准确字节及SHA256；不能只重序列化几个字段后丢来源。

Controller在Pod创建前直接重读原VM/immutable Secret，核对UID/完整Spec、原owner
与pristine日志，验证resource与native预算、管理EdDSA公钥一致；用固定flag给
Runner。Runner有界解压并核对全部启动身份，再写入只读runtime ISO。此处来源
是Controller已认证输入的投影；手工flag不构成Kube授权，也不替代最终Runner/QMP
持久fence。这里只允许首次pristine启动，已有owner/history的重启须受控恢复。

Guest sysinit从固定只读ISO读取来源，实际检查ISO文件系统及只读标志，封存准确
输入/固定配置（已有不覆盖）、创建root私有状态目录与root cgroup、启用memory
controller，再调用前批已实现的Bootstrap落实/读回实际限额。保留部分写和原日志。
vmstart与每次PG出生独立重读ISO/封存/固定配置、原Bootstrap承诺及当前Held；
不能让provision失败后误用旧配置，不能凭ready文件或旧Applied放行。

受管Guest关闭根cgroup进程/线程迁移权限；Kernel只读标记`neonvm.dsga=1`与残留
来源/状态使模式保持收紧，输入丢失/出错不退回legacy。新原位PG入口是唯一特权
出生路径。产品构建源码编译静态程序并以root SUID模式放入Guest，Makefile和
daemon Dockerfile改用Go包路径，避免把新测试文件通配进构建。

旧neonvmd未认证控制通道在受管模式禁止CPU写和任意文件路径，模式单调保持。
保留CPU只读与**固定**`/run/neon/management`官方TLS供给/轮转，且只允许cert/key/CA
三个键、64KiB请求、严格JSON及root控制目录；不能借TLS上传修改封存输入、
sudoers或cgroup。这没有把旧TLS通道说成新的认证资源服务；独立root资源认证/
持久fence通道仍须继续实现。TLS真实正向供给/证书轮转和隔离须Linux后验。

## 实际Mac检查

工作区证据`build/dsga-provision-as-final/`：23个代码/构建/模板输入在最终检查前后
SHA一致；不含所有者原有AGENTS.md改动，未提交该改动。

- 定向race 7个顶层测试、**28个含子项pass事件，0fail/skip**：新增来源字节/
  身份/公钥/预算匹配、有界解压、残留/Kernel模式、原承诺绑定及旧写拒绝；
  受影响immutable CheckReady与已有出生主路径。未重复未变PG/SDK/其他并发套件。
- 资源/启动/daemon四个相关包Mac vet exit0；guard、daemon、Runner、Controller
  的Linux amd64及arm64交叉编译均exit0，共8次构建。没有执行这些Linux产物。
- 实际Go模板渲染vmstart后`sh -n`通过；vminit原文本`sh -n`通过。不把语法解析
  称作执行了BusyBox或Guest启动。
- Mac运行的是typed业务输入、真实私有日志文件与HTTP路由拒绝，未监听TCP；
  Kube对象/内核/exec边界仍是明确夹具，没有验证K8s API、RBAC、Pod或物理行为。

最初Runner/daemon普通沙箱编译遇到尚未缓存的锁定依赖及模块缓存写权限，获取
依赖后最终构建/测试通过；原日志保留。中途一次Runner编译输入未冻结，只作
诊断，不当最终证据。最终源码单独冻结后完成上列检查。

## 业务缺口与目标环境边界

仍须编码：控制面原计划显式资源profile和第二配置文件的生产接线，Neon补充
sysinit不得重新委派cgroup、受管Compute停止旧未认证Monitor/LFC调整；独立
认证root服务、完整PrepareBudget/GuestReady/FinalizeBudget/首次准入、排空、
Host/最终VM执行及公开worker/生命周期、跨PG/Guest/owner关闭恢复。
这些不能归为仅Linux待测，完整Goal仍active/incomplete。

Linux后验：实际镜像/root权限/SUID/nosuid、原UID/immutable输入到Pod/Guest完整链、
只读ISO/root文件fsync掉电、CG成员/迁移/全线程降权/PID信号、TLS正向供给、真实
限额/整个HugeTLB池/物理释放、Runner/QMP/Host闭环和性能。外部System/Host上界
仍由实际承诺生产者建立，初始SourceConfig数字本身不是完整预算或VM许可。
