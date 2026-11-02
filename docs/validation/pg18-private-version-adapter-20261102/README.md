# PG18 私有版本适配阶段

问题：Root出生与normal原来源原先硬限定PG17。新增私有ABI选择，仅接受已实现的17/18；PG17旧字节不变，PG18仍必须绑定自己的原ELF、policy、build及过滤器，不改公共wire。

验证：a797ec的5个race根/14子项与CLI、Darwin、native编译通过。原PG18 ELF因RPATH被正确拒绝；保留该失败。c83745复用同806对象仅去发行RPATH重链，原对象/ELF不改，真实C ABI/filter组件及错误布局负控通过。

边界：测试中range地址是fixture，并非真实PG映射。此提交不签NativeFinal、normal降限、VM或端到端合格；PG17发行锁不动。完整原运行与失败留Linux /data，小收据记录准确路径和SHA。
