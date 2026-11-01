普通控制2固定目录权限与工作目录：组件阶段

原真实layout要求network的原DNS inode保持60003:60003/0644供PG及其它服务读取，Root stock udev父目录保持0:60004/01771。旧泛private判定要求role所有且other位为0，会恒拒这两个真实合同。
现在只从准确固定角色和PrivatePaths成员中选择：DNS仅regular单link0644，授原inode读写/截断，不给父目录替换权限；/run/udev仅Root sticky01771，不泛chown、不给Root能力。其余目录逐固定layout核exact uid/gid/mode/type。原FS/layout/stock实际来源及持续重核仍不可缺。
显式ordinary2在原Gate源核与cap前检之后固定cwd为/并真实Getwd复核，避免继承Root私有cwd使普通服务相对操作失败；旧0、Root、Compute分支不变。
最终410ad6准确Git8aac+3源，包含Go及原汇编/C头等实际编译输入1579项；三race根、40子项、CLI及Darwin编译通过，含真正子进程继承其它cwd后切到/。独立只读审查无组件级阻断。未运行九服务/全Gate/PG Source，不签normal或端到端。
首轮c475fc只取Go漏原汇编，导致missing function body；零实际测试。保留原失败与新完整图，不更改原失败、不用编译进程0当测试绿。完整ELF和缓存保留/data/neon-build/validation，不提交二进制。
