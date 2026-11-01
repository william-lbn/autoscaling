固定管理TLS空目录准备原语（组件）
原17mount图/run tmpfs不会从镜像保留目录；原actions构造只校验/run/neon/management，且生产无mkdir供给。本原语只在同R2的stock durable intent之后由后继producer调用，固定创建空neon/management及逐FD/父目录fsync；原始/run FD须Root0:0/0755、可写TMPFS/CLOEXEC。所有既有目录、symlink、部分前缀均拒，不采纳证书、不删除、不重试。
后续官方三文件AtomicWriter供给与轮换协议不变。此函数不签发Source，调用者仍须原g/FS2/三actor/prepared及失败tombstone。
真实BuildKit Linux tmpfs低层3race根5子例通过，Guard/Builder Linux/Darwin编译0；不含原Guest17graph/stock生产者执行，不计S/E，也不证明完整normal。
