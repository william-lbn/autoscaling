PG17内存控制来源最终独审与准确重跑（2026-11-01）

初始6源/950输入/5U11sub/ce0dce真实S及全部失败在相邻normal-pg-memory-control-source-20261101保留。初始freeze是历史收据，其中2源码的原哈希对应初始版本，不把它转签为当前6。独审发现解释器固定IP=0不能推导任意IP拒绝；当前严格禁止LD_ABS读取seccomp_data的IP低/高字。原b95完整程序只用arch/nr/args，不需要IP，因此不削弱原过滤器。新低/高IP回归同时检查validator/evaluator均拒绝。

实际Git5acb+仅6源、970 COPY全部匹配；6race根11子项通过，其中5根为直接回归、仅新增1个根。新Go d8b848c...在实际369 KVM重跑唯一原S根，Guest0/QEMU0、原PG正常Wait0。C探针1a0241...字节未改；原3继承对象真正消失，完整798/791/531过滤器来源、Native198握手、四数组真实SQL扩缩与2000行checksum均保留。33机制中THP开关清零/huge虚拟预约仍实际允许，不触huge页；依赖尚待组合的Kernel控制，不能称已经全面禁止huge。

本次是相同S ID的新图重跑，不能与初始S计成两个不同覆盖。新native driver事先修正base=5acb与new_production_and_test_sources名称，旧错误元数据原样保留并有erratum。原stdio类型检查不等于typed来源，连续资格始终Unknown；未删MissingSources，无完整normal硬降限/Linux E/K8s E。
