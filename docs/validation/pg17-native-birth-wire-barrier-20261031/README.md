# 出生wire FD验收的子进程同步修复

真实6.12.26 KVM首轮S-WIRE-001失败，rights子例FD计数10→9；Guest exit1，Host/QEMU虽0也不能算通过。生产FD清理代码未见泄漏，测试fixture的nobody子进程发送后退出，os/exec后台stdio复制管道在计数期间关闭，既能误报又可能抵消真实泄漏。原失败目录和仓库attempt-01完整保留。

本批只改native测试：rights/truncated-rights子进程发送后停在有deadline的明确同步屏障；父端在子进程/stdio/pidfd持续存活时保持严格FD数相等，之后发测试完成字节并严格Wait退出。没有改为<=或加sleep掩盖竞态，没有改变生产parser/peer/FD处理。测试完成字节不是112B生产出生ACK。

Host Linux6.8/Go1.24.3/race/count=1仅直接受影响S根10子例PASS，精确COPY manifest和命令同目录。原3个U根源码及依赖未变不重复刷测。目标6.12.26 Guest须以新准确源码重编后再验，当前未声称目标内核通过；没有PG/normal/VM/K8s授权。
