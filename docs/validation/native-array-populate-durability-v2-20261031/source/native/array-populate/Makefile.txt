# 独立默认不安装；原runtime builder后继须精确引用ELF/SHA，不自动修改现镜像。
CC ?= gcc
CFLAGS := -std=gnu11 -Os -Wall -Wextra -Werror -ffreestanding -fno-builtin -fno-stack-protector -fno-pie -fno-asynchronous-unwind-tables -mno-sse -mno-mmx -msoft-float
LDFLAGS := -nostdlib -static -no-pie -Wl,--build-id=sha1,-z,noexecstack,-z,max-page-size=0x1000,-z,common-page-size=0x1000
all: neon-dsga-array-populate
neon-dsga-array-populate: array_populate.c
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@
clean:
	rm -f neon-dsga-array-populate
.PHONY: all clean
