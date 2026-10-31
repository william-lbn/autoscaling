该目录是显式接入前的固定数组预触者，默认不安装、不改现有镜像。

只能由原Root持同资源OFD与append-only pending，使用4个已验证shmem FD，通过Go的原子CLONE_INTO_CGROUP/PIDFD入口调用。
helper没有libc/heap/线程/网络；进入GO前锁全部PT_LOAD与64K固定栈，后续仅4准确MADV_POPULATE_WRITE范围及固定控制FD。
完整源与真实369组件证据见 [验证档案](../../docs/validation/native-array-populate-20261031/README.txt)。

构建：GNU12.2、Linux amd64，`make`。原发行镜像必须后继精确绑定ELF/source SHA；不能单独执行即视为Root资格。
本批没有完整Source/normal/VM许可；普通System图与所有cap写者pending门禁尚未完成。
