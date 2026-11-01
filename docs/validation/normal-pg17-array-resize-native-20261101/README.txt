PG17真实四数组Grow→Shrink组件（2026-11-01）

这是新增TestNativeNormalPG17ArrayGrowShrink，独立build tag，生产源一字未改。旧f881只AuthorizeArrayRequest，不能称Grow/Shrink；本批另立PG17-S-ARRAY-RESIZE-001并实际执行四笔公开Neon SQL：Grow1024→Shrink256→Regrow2048→Shrink256，容量代次1→2→3→4→5。Root原daemon Begin/SUID保原UID1000→NativeBase/Final/同PID迁private/released持久→四map_files原inode→原arrays memcg helper全部实跑，Root body-bound专用JWT及四次预触仍原生产门禁。未装test_dsg_region，未替换Provider，未绕过Source/unsafe拒绝。

正式镜像47ef/pkglibdir=/usr/local/lib：PG b95的ELF9f3，原neon.so1e9468...确有管理符号；只复制该同镜像原neon.so及psql的实际loader DT_NEEDED闭包到独立RO/usr.ext4，逐路径/SHA和--list留档。没有启用无关39扩展，没有把运行插件库塞进旧pre-main16 manifest（会违反unreachedELF合同）；正常最终Source仍须独立纳入插件闭包与配置/能力，本批不是Source13授权。CREATE EXTENSION neon使用原control默认1.17并完整原SQL升级链，不手造C别名。旧Rust测试夹具硬选1.8无shared_cost，首真实S已Grow成功后在成本查询失败，保FAIL/Guest1/QEMU0；改正确正式版本后新ELF/nonce完整通过，不掩盖旧失败。

每笔管理回执通过公共control.Request/ControlResponse完整Key/context验证；真正调用neon_dsg_request_resize，然后独立事务neon_dsg_resize_status与neon_dsg_shared_cost。每个活prefix先mincore逐页验证resident，之后只读fault同原FD并逐PFN→kpagecgroup核原A inode；若洞由System重分配必失败。Shrink尾部仅不fault的mincore证明洞，四个Provider都须当前verified/physical_release_verified且无partial/unknown、成本准确页向上对齐。2000行真实堆表与checksum贯穿四笔不变。尾洞或回执不是完整uncharge/free/normal证明；仅验证本次PG17直接功能。最后原PM pidfd发SIGTERM、实际cmd.Wait0，再停Root服务；不是只看到进程退出码片段。

准确AS Git f553f07+唯一新测试，build66292d实际945COPY/nativecompile/export全0；这些ENV不计U。最终GoELF00cbcf4b...在新09debc准确369/notes7cf KVM enabled/present实跑，Guest0/QEMU唯一Wait0、22.64s Go/41.87s含报告。原256MiB出生、8MiB block、实际QMP到1536MiB，2CPU；BuildKit与Guest外层/容器均2GiB/2CPU，无网/Host配置不变，八份分块SHA报告完整。全部首次编译错误及1.8错误版本S保留。

明确未覆盖：journal StartCAS（resourceguard同包不能import pgexec/journal，会循环；后继需独立pgexec测试driver与真实SQL HTTP适配）、正式Rust DsgSqlClient、真实Host Node/issuer、全部运行插件Source、normal持续费用/硬域降限、FS/角色来源全组合、K8s/E。直接SQL为正式公开入口，但不把它自称整个agent执行链。Host和issuer是TEST_ONLY fixture；普通normal五拒绝未动，E0。4笔操作是一根新S，不分环境/内部步骤重复计数。
