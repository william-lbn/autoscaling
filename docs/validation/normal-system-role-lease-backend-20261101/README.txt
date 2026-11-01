原System角色main租约后端 / 2026-11-01

范围仅2新增Linux源码，不改原API、Service/Spawn/Gate/journal，也不修改RootAction、
旧Root-v1权限或公共PG中立ABI。本模块是私有发布/读者后端，不是完整Source或normal许可。

真实来源接线只接受P2 opaque InstalledMain。原第二stop+fullcontrol/stdio+三WAL成功后，
原resource flock内prepare→candidate(无权限)；P2实际detach→Installed phase→activate；
active后才释放原flock。所有读者先resource→Authority独立producerMu→Task，不取broker/exec。
getter每次从原Installed重核Root/source/三WAL见证、同Guard/身份/单调floor/state与lock inode。
主角色matcher每包核原SO_PEERPIDFD PIDFS内核对象、kernel cred PID/UID/GID、原proc start/boot、
原clone与新的phase pin同对象、原creator线程真实活。继承连接的fork子PID不能当main。
Host6.8 anoninode不同pidfd可同inode，因此独立拒非PIDFS，不按numeric PID fallback。

新phase仅真实producer能构造。普通role UID/GID600xx按原固定表核；旧UID0 Gate任务不改。
backend准备时独立dup phaseTask和typed creator PIDFD_THREAD；读者handle再拿独立dup且close。
同role替换只允许原Child唯一Wait的真ProcessState/纯原ExitError+creator真退役终态，不能只
根据done、pidfd可读或WAL JSON。Unknown不可active/复用nonce，候选失败不删历史槽位洗账。
退出前缀仍逐inode/ctime/hash核；未确认退出的原Task/creator pin保持有界保留。

final07d5f1：base eaf2243 + only本2/P2API2；1058实际Go输入COPY逐hash0，6race根30命名子项
全PASS/零skip，1.142s；Linux resource-guard CLI/Darwinarm64包编译均0，RUN内实际2CPU/2GiB。
旧cd11b4(6/29)保留原义；其后仅新增原clone独立对象比对+1负例。所有原receipt未覆盖。
6根中引用/状态是模型；WAL测试调用实际file+dir fsync和原reader，非硬件耐久故障实验。
Host实际pidfd仅核不支持明确拒绝/负例，不把无实际InstalledMain constructor的对象当正控。

独审发现与P2修复：旧Wait确认读已Close creator File会永久Unknown，P2现在prepare捕获原
独立CreatorPin后读真实退役；后继actual lane仍必须retire=true使M真正退出，旧v1不动。
新的P2真实constructor/停点hook/Guest正控尚未接入本cohort，不能把实现了reader接口当资格。
待P2合图真实candidate→Detach→active/RootAction主与fork负例之后才能扩大本组件验收。
不删除MissingSources，不称完整normal降限、真实PG或K8s端到端通过；S0/E0。
