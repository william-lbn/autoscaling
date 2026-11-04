# 初始启动授权中的StateClaim纯读取有界等待

真实f0eb实例已执行Guest内核并进入Root initial-boot，后续Runner返回QEMU signal:killed并合并StateClaim GET 5秒超时。主main.go只记录错误；初始drive错误会终止其原QEMU句柄，退出留证另走dsga-x，普通Tick发布错误仅Warn。原日志和完整调用图支持定位初始授权读取路径，但不证明具体第几次授权或最终OOM/外部kill情况。

修复只给实际Start之后的原initial drive附私有context来源。它继续使用原3分钟runCtx和原flock，普通memory操作门保持关闭。确切StateClaim GET的暂态transport/timeout、429或5xx失败可在原ctx里最多3次重新读取完整授权链；每轮最早复核冻结Lease UID、holder及journal owner/epoch，再核Birth、State、Pod和Node。上下文结束、租约过期、403/404、任意身份漂移立即原失败，不给Unknown发许可。

循环内部只有纯读取与20ms原ctx等待，不重跑drive阶段、prepare、QMP、CAS、Start或任何写操作，不续既存deadline，不退回旧publication。普通ctx/Tick/publish沿原单次失败合同；初始StateClaim错误日志只给固定stage/causecode，Unwrap保原错误链，不打印URL、响应正文或凭据。公共PG API/源证明、Host预算与Runner权限均未变。

验证只针对本改动：相同冻结身份经历一次暂态后完整fresh读取成功、owner/Lease/State/Pod漂移拒绝、403/404/不适用ctx不重试、三次上限与原deadline/cancel、写操作计数不增加。实际测试结果及准确Runner产物见receipt；工具启动失败不记测试失败或通过。大日志与旧失败留/data，仓库仅保存非敏感重现和小票。

发布边界：源码单元与Runner编译通过也不等于K8s启动、DNS、SQL或安全normal降限通过。只有修复Runner实际预载和正式保数据重建后，才沿原链验证Root→DHCP→Compute→PG→内存伸缩；不重置旧VM或出生账本。

实际验证补充：首0cd工具未进入Go（缺DOCKER_CONFIG导致buildx不可见），仅修执行环境。5b真实5根race中，epoch负例用同holder Takeover产生合法no-op，未真正升代；原整轮FAIL保留，生产代码未改。随后只改该测试生产者为原Store两次CAS A→B→A，同holder得到更高epoch，并要求待测授权零额外写；561仅复验该失败根及2个直接受影响已有根，3根14通过事件，失败/跳过/竞态均0。前4新绿根沿原同生产源码结果复用，不重复计子例或把旧5b整体改绿。正式Runner CGO0/trimpath/s-w构建0，产物9389fd50…；实际3Gi/3CPU/swap0，峰值594608128字节/OOM0。

最终独审2731ce确认：去重7根30通过事件（四个旧完整绿根16、新三根14），原5b整轮FAIL不改。561容器内实际11673项源码检查，另18个历史docs库存文件因同basename过滤未进该轮清单，但此前5b全11691已核、新树再逐18SHA与Git核一致；无Go或运行期embed缺失。准确计数与补票见receipt，不把容器内实际数写成11691。真实Runner和Guest/K8s启动仍须部署验证。
