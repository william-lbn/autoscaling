# normal-split-metadata-v1 独立Guest内核

从准确369 resolved配置11344757复制，只显式启用默认关闭的`CONFIG_NEON_DSGA_CONTROL_METADATA_GUARD`。新0008留在本profile目录，旧公共patches循环和旧所有文件均不变；未动Go PhysicalKernelBuild、发行锁或镜像标签。

只在三个用户VFS入口要求初始user namespace的CAP_SYS_ADMIN：kernfs mode/uid/gid setattr、security/trusted xattr set、user xattr set（后二者含remove）。文件/目录/symlink共用setattr路径。内部kernel属性更新不加门；proc_sys本来拒mode/uid/gid，普通devtmpfs/ext4/tmpfs的udev/TLS合法metadata不受此门影响。其它系统调用、Root/PID1/Compute和future writer来源仍须独立验证，不能把这项当完整normal安全。

`build.py --output /data/neon-build/validation/<独立nonce>`从仓库准确基线文件重建原369的6+split7缓存上下文，再新apply8和make-j4；内层4CPU/6GiB/无网新步骤。须先source既有env.sh，且外层systemd也设有界资源/超时。记录原失败与actual tuple；默认关闭配置和新resolved配置的语义diff必须只有该新symbol。实际58603b构建235.176s通过；原6+7 make全部CACHED，新phase4CPU/6GiB/j4。新resolved唯一语义差为该symbol，default-off与原369语义完全相同。真实304d4f KVM 3.565s/Guest0/QEMU0：独立C新建nonce空cgroup，UID0五cap集合0/NNP1/Seccomp0的16元数据请求全部EPERM且原对象属性/xattr不变，普通ext4/tmpfs 18正控成功、Root正控/清理成功。实际LSM为lockdown,capability,landlock,yama,apparmor。没有PG/生产Gate/九真实服务，不是完整normal认证；config-source仅记录准确实际tuple，旧369/Go表/发行锁保持原样。
