stock udev 原始RO输入原语 / 2026-11-01

新2源仅读取已有opaque System3：固定 /etc/neon-dsga/ordinary-stock-udev-manifest.json，
要求原System3显式纳MF及原70库存；不接受路径、Source布尔、Root命令或调用者补SHA。
依赖Root8不可变8e37b8的shared schema/库存及MF-gated System3扩展；旧manifest_linux.go
必须删除而不是叠加同名common定义。旧1/2和缺MF的旧3不新增权限；Inputs缺原MF保持Unknown。

同原System锁内，逐原Path/CanonicalPath/Kind/Code/Bytes/SHA关联，钉实际RO regular FD，
记录dev/ino/原mountID/ctime/mtime/mode/UID/GID并前后hash一致；逻辑/canonical全部祖先RO。
重验既比较原heldFD又重开准确原路径，不以同字节新inode冒原对象。MF自身只由System引用，
prep MF不含自身hash或SystemSHA循环。ref可带System原binding/引用摘要，JSON不能反构opaque。
规则digest覆盖原27+99virtio+ACL。原passwd/group只从钉住FD的SectionReader读取，复核不改
共享offset；要求11符号组本地文件唯一有效解析，不调用os/user/NSS，不打印账户原文。

临时独立视图调用原System.readClosure重新核实际完整ELF闭包，不借Observed旧bool或只数DSO。
所有新打开ELF对比原System heldFD对象；stock单独要求原musl解释器和每个DT_NEEDED实际
默认首lookup在prep MF中，全部prep DSO必须被stock程序/helper实际到达。其它System角色DSO
不强塞stock MF。实际/lib/udev、/etc/udev完整静态目录逐项核，未知文件/子目录、遗漏helper/
rules/hwdb/optional.bin、额外alias子树均Unknown，持目录FD复核原mount/inode。没有扫描/run。

锁序resource→Inputs.mu→System.mu；不在System.mu内调用snapshot/referenceSHA锁方法，
不回调FS reference。close只关本对象FD；源对象关闭后recheck拒。snapshot仅缓存深副本，
生产使用前后必须显式recheck，不从缓存引用/JSON产资格。

真实首cohort d8cbc0：Git97d90bc+Root8e37b8不可变8/deleted1+仅Inputs2，1360实际Go输入
COPY逐hash一致；新5race根35命名子项+旧stock7根70子项=12/105，全部PASS零skip。
Linux CLI/Darwinarm64编译0。单BuildRUN显式3CPU3GiB，实际memory.max3221225472、
cpu.max300000 100000，Go2/768MiB；预算uniqueWait+原unit终态后释放，不把wrapper0冒测试0。
新U正控只纯账户/库存关联、FD pread语义与缓存副本；没有制造opaque真实System正向。

当前S0/E0/Sourcefalse/normalfalse。原3actor与nscd/socket、/run/udev/rules.d保护由Root
后续原producer核；真实stock exec/wait/完整事件处理、投影seal/移交与未来写者来源均未签。
本包不改这些来源，不将tmpfs写目录称RO，不冒称actual stock服务已跑通。
