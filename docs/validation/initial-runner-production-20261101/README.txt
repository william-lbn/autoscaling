正式Runner和Guest CLI启动接线（静态入口与编译验收）
同原RuntimeAssets在Initialize前Decode/严格derive，新Doc2明选base256MiB且不超过BootUpper；原QMP后继仍必须实测。单slot/Max-base/node0/block8/monitor none与固定BootChannelArguments实际挂接；旧v1几何分支保留。缺源/漂移不得退旧。固定CLI仅real root两动作，完整Guest后置Check保持。
Git12742c8+六源，实际3035个非.github文件COPY核hash（含非Go embed；不冒称CI文件也逐项核过），RUN2CPU/2GiB/no额外swap；race6根20子例（新3根+直接Boot3）与Runner/Guest静态编译全0。独审报告随附。未运行完整生产buildQEMUCmd网络/磁盘，也不是正式镜像Guest/PG/normal/Helm端到端验收。
