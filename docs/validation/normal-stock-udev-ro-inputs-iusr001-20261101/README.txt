IUSR001 原Inputs2额外静态规则孔补充 / 2026-11-01

本后继只改Inputs2，原37、d8cbc0原日志/源副本/manifest完全保留历史，不重新签旧hash。
Kernel独审准确udev-rules.c SHAa1ceb115a85c3522553443f605cd936f7ec5e30a88c9b48ae30b3514ba617757
rules_dirs51..61发现实际--enable-split-usr还查/usr/lib/udev/rules.d及无条件/usr/local/lib/udev/rules.d。
不能把只核/lib与/etc称完整静态规则lookup封存。该缺口没有转签成实际绕过/实际Source通过。

复用Root shared NormalSystemStockUdevStaticRuleDirectories唯一固定四根表；/run单独由RuntimeDirs
负责，不另开任意MF规则路径。原/lib和/etc逐MF/原目录FD完整覆盖；两个额外根只能：
1 当前canonical就是已钉原规则目录且真实dev/ino/mountID/Readonly对象完全同；允许原已知规则内容。
2 独立根为真实RO目录且空，持原目录FD；3 真缺失，持最近实际RO父目录FD，并核完整祖先。
每次recheck重新解析完整lookup状态/逻辑canonical并同原pin/引用比对；新目录/新inode/非空shadow/
RW空目录/断链alias/没有原pin均Unknown。缺失不是省略metadata，需要实际路径不存在与RO父证据。
规则表不从调用者/采样推，所有输入仍非授权；同source原pin关闭/根/namespace与其它Sources由原链核。

新精确a57172：Git97d+Roota1f4d9 immutable10/deleted1+仅后继Inputs2，1360实际COPY逐hash一致。
新增006纯策略U1/6case，原Inputs5/35与stock7/70直接回归，总13根111命名子项race全PASS零skip；
LinuxCLI/Darwinarm64编译0。未把策略model RO对象当真实Guest readonly正控，S0/E0。
实际3CPU3GiB单RUN/Go2/768MiB、原唯一Wait及原unit终态，详原receipts，不把过程0冒测试0。

Root后继actual stock prepare仍需原3actor/nscd、/run保护与真实完成/投影seal，Kernel正在查IUSR002
实际kmod静态编译与模块/config/defaultlookup，未闭不能Source。此修不泛加任意模块路径白名单。
原37文档和source manifest均是当时图；本union新review-source-manifest才映射最终current2。
旧源码准确副本可在原owned-source/*.txt核其95cb/5f6，不用当前后继路径去误验历史源。
