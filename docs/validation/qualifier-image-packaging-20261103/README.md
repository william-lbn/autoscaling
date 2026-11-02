# 资格发布器静态镜像打包验证

新增dsga-platform-qualifier/Dockerfile，复用既有Go base，显式模块/cache目录及有界构建并行，RUN禁止网络；最终scratch镜像仅复制静态CLI，ENTRYPOINT与Helm真实--config参数一致。root身份仅为原私有输入/Unix socket所有权需要，无额外capability，报告与凭据不写入镜像。

Linux按准确HEAD063243c及本配方构建、docker load成功；真实只读scratch容器在无capability/无网络下执行，缺--config按合同退出2。这个拒绝是运行时打包正控，不是资格签发成功。初次测试因工具镜像默认Go路径未匹配cache而下载失败，原日志保留；本配方明确GOPATH/GOMODCACHE/GOCACHE后验证通过，没有改代理或清缓存。

该TEST_ONLY镜像已在worker-118的Docker中可见：docker images neon-test/dsga-platform-qualifier。准确tag/ID见receipt.json；镜像原tar在/data独立验证目录，没有提交二进制。它基于已验证发布器阶段源码，不表示最新Agent/owner统一发行镜像。正式G5及平台Source通过后还须从最终冻结源码重建并锁定全部镜像，再经SSH/containerd k8s.io导入三节点。
