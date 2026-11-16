# normal内核只封装本次实际通过原四元组核验的文件，禁止复用普通vm-kernel身份。
FROM scratch
COPY vmlinuz /vmlinuz
COPY kernel.config kernel-notes.bin kernel.release actual-source-SHA256SUMS memory.max cpu.max compiler.txt linker.txt toolchain-packages.txt /kernel-receipt/
