ARG UBUNTU_IMG_TAG=replaceme
ARG UBUNTU_IMG_SHA=replaceme
ARG ALPINE_IMG_TAG=replaceme
ARG ALPINE_IMG_SHA_AMD64=replaceme
ARG ALPINE_IMG_SHA_ARM64=replaceme
FROM ubuntu:$UBUNTU_IMG_TAG$UBUNTU_IMG_SHA AS check-arg
ARG KERNEL_VERSION
WORKDIR /build

# Force initial check that KERNEL_VERSION is set appropriately
RUN set -e \
    && echo "Build linux kernel ${KERNEL_VERSION}" \
    && test -n "${KERNEL_VERSION}" \
    && echo "force this as a requirement for build-deps" > /build/arg-check-succeeded

FROM ubuntu:$UBUNTU_IMG_TAG$UBUNTU_IMG_SHA AS build-deps
WORKDIR /build

RUN apt-get update && apt-get -y install \
    curl \
    ca-certificates \
    build-essential \
    gcc-x86-64-linux-gnu \
    gcc-aarch64-linux-gnu \
    flex \
    bison \
    libelf-dev \
    bc \
    libssl-dev \
    python3 \
    cpio \
    zstd \
    libncurses-dev

# Only require check-arg at this point, so the 'apt-get install' above is definitely cached.
# We need to copy something in, otherwise 'docker buildx build' will just completely drop the
# check-arg stage.
COPY --from=check-arg /build/arg-check-succeeded arg-check-succeeded

# the ARG must be down here (specifically: not before 'apt-get install'), else docker seems to
# assume that KERNEL_VERSION could influence the 'apt-get install' and refuses to cache it.
# (maybe they're just exposed as environment variables to RUN?)
# This all as of Docker 26.1.4 - 2024-06-07.
ARG KERNEL_VERSION

COPY echo-source-url.sh echo-source-url.sh

RUN set -e \
    && rm arg-check-succeeded \
    && mkdir -p linux-${KERNEL_VERSION} \
    && echo "downloading linux-${KERNEL_VERSION}.tar.xz" \
    && curl -sfL "$(./echo-source-url.sh "${KERNEL_VERSION}")" -o "linux-${KERNEL_VERSION}.tar.xz" \
    && echo "unpacking kernel archive" \
    && tar --strip-components=1 -C linux-${KERNEL_VERSION} -xf linux-${KERNEL_VERSION}.tar.xz

# Apply any patches
COPY patches patches
RUN set -e \
    && cd linux-${KERNEL_VERSION} \
    && for f in $(ls -1 ../patches | sort); do echo "Applying $f..."; patch -p1 < ../patches/$f; done


### Cross-compilation related steps

# Build the kernel on amd64
FROM build-deps AS build_amd64
ARG KERNEL_VERSION
ARG AMD64_KERNEL_CONFIG=linux-config-amd64-${KERNEL_VERSION}
ADD ${AMD64_KERNEL_CONFIG} linux-${KERNEL_VERSION}/.config
ARG VERSION_SUFFIX
RUN cd linux-${KERNEL_VERSION} \
	&& make ARCH=x86_64 CROSS_COMPILE=x86_64-linux-gnu- EXTRAVERSION="$VERSION_SUFFIX" -j 8

# Copy the kernel image to a separate step
# Use alpine so that `cp` is available when loading custom kernels for the runner pod.
# See the neonvm controller's pod creation logic for more detail.
FROM --platform=linux/amd64 alpine:$ALPINE_IMG_TAG$ALPINE_IMG_SHA_AMD64 AS kernel_amd64
ARG KERNEL_VERSION
COPY --from=build_amd64 /build/linux-${KERNEL_VERSION}/arch/x86/boot/bzImage /vmlinuz

# Build the kernel on arm64
FROM build-deps AS build_arm64
ARG KERNEL_VERSION
ARG VERSION_SUFFIX
ADD linux-config-aarch64-${KERNEL_VERSION} linux-${KERNEL_VERSION}/.config
RUN cd linux-${KERNEL_VERSION} \
	&& make ARCH=arm64 CROSS_COMPILE=aarch64-linux-gnu- EXTRAVERSION="$VERSION_SUFFIX" -j 8

# Copy the kernel image to a separate step
# Use alpine so that `cp` is available when loading custom kernels for the runner pod.
# See the neonvm controller's pod creation logic for more detail.
FROM --platform=linux/arm64 alpine:$ALPINE_IMG_TAG$ALPINE_IMG_SHA_ARM64 AS kernel_arm64
ARG KERNEL_VERSION
COPY --from=build_arm64 /build/linux-${KERNEL_VERSION}/arch/arm64/boot/Image /vmlinuz

# Dummy default target without target architecture
FROM ubuntu:$UBUNTU_IMG_TAG$UBUNTU_IMG_SHA
RUN echo "No target architecture specified, can't build kernel image" && exit 1


FROM build_amd64 AS test_kernel_receipt
RUN --network=none set -eu; mkdir /test-only-receipt; \
    cp /build/linux-6.12.26/.config /test-only-receipt/kernel.config; \
    cp /build/linux-6.12.26/include/config/kernel.release /test-only-receipt/kernel.release; \
    sha256sum /build/linux-6.12.26.tar.xz /build/linux-6.12.26/vmlinux /build/linux-6.12.26/arch/x86/boot/bzImage > /test-only-receipt/build-SHA256SUMS; \
    objcopy --dump-section .notes=/test-only-receipt/kernel-notes.bin /build/linux-6.12.26/vmlinux; \
    gcc --version > /test-only-receipt/compiler.txt
FROM scratch AS test_kernel_export
COPY --from=test_kernel_receipt /build/linux-6.12.26/arch/x86/boot/bzImage /vmlinuz
COPY --from=test_kernel_receipt /test-only-receipt/ /


# 仅此新stage修改7patch/profile；上方旧6patch make命令/输入/flags完整保留。
FROM build_amd64 AS normal_split_build
COPY split-patches/0007-Neon-mm-Bound-hierarchical-page-counter-cancel-IRQ-gap.patch /build/patches/0007-Neon-mm-Bound-hierarchical-page-counter-cancel-IRQ-gap.patch
RUN --network=none set -eu; cd /build/linux-6.12.26; patch -p1 < ../patches/0007-Neon-mm-Bound-hierarchical-page-counter-cancel-IRQ-gap.patch
COPY linux-config-amd64-6.12.26 /tmp/kernel-default.config
RUN --network=none set -eu; cd /build/linux-6.12.26; make ARCH=x86_64 CROSS_COMPILE=x86_64-linux-gnu- KCONFIG_CONFIG=/tmp/kernel-default.config olddefconfig; grep -qx '# CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER is not set' /tmp/kernel-default.config
COPY profiles/normal-split-v1/linux-config-amd64-6.12.26 /build/linux-6.12.26/.config
ARG NORMAL_SPLIT_VERSION_SUFFIX
RUN --network=none cd /build/linux-6.12.26 && make ARCH=x86_64 CROSS_COMPILE=x86_64-linux-gnu- EXTRAVERSION="$NORMAL_SPLIT_VERSION_SUFFIX" -j 8
FROM normal_split_build AS normal_split_receipt
RUN --network=none set -eu; cd /build/linux-6.12.26; mkdir /normal-split-receipt; \
    grep -qx 'CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER=y' .config; \
    cp .config /normal-split-receipt/kernel.config; \
    cp /tmp/kernel-default.config /normal-split-receipt/default-off.config; \
    cp include/config/kernel.release /normal-split-receipt/kernel.release; \
    objcopy --dump-section .notes=/normal-split-receipt/kernel-notes.bin vmlinux; \
    objcopy --strip-debug vmlinux /normal-split-receipt/kernel-symbols.elf; \
    nm -n vmlinux > /normal-split-receipt/kernel-symbols.txt; \
    objdump -d --disassemble=page_counter_try_charge vmlinux > /normal-split-receipt/page-counter-try-charge.asm; \
    objdump -d --disassemble=page_counter_uncharge vmlinux > /normal-split-receipt/page-counter-uncharge.asm; \
    sha256sum init/Kconfig mm/page_counter.c mm/hugetlb_cgroup.c kernel/pid.c include/linux/pid.h kernel/sysctl.c /build/patches/*.patch .config arch/x86/boot/bzImage > /normal-split-receipt/actual-source-SHA256SUMS; \
    x86_64-linux-gnu-gcc --version > /normal-split-receipt/compiler.txt
FROM scratch AS normal_split_export
COPY --from=normal_split_receipt /build/linux-6.12.26/arch/x86/boot/bzImage /vmlinuz
COPY --from=normal_split_receipt /normal-split-receipt/ /

FROM normal_split_build AS control_metadata_build
COPY profiles/normal-split-metadata-v1/0008-Neon-kernfs-Require-admin-for-control-metadata.patch /build/patches/0008-Neon-kernfs-Require-admin-for-control-metadata.patch
RUN --network=none set -eu; cat /sys/fs/cgroup/memory.max; cat /sys/fs/cgroup/cpu.max; test "$(cat /sys/fs/cgroup/memory.max)" = 6442450944; cd /build/linux-6.12.26; patch --dry-run --fuzz=0 -p1 < ../patches/0008-Neon-kernfs-Require-admin-for-control-metadata.patch; patch --fuzz=0 -p1 < ../patches/0008-Neon-kernfs-Require-admin-for-control-metadata.patch
COPY metadata-baseline.config /tmp/control-default-off.config
RUN --network=none set -eu; cd /build/linux-6.12.26; make ARCH=x86_64 CROSS_COMPILE=x86_64-linux-gnu- KCONFIG_CONFIG=/tmp/control-default-off.config olddefconfig; grep -qx "# CONFIG_NEON_DSGA_CONTROL_METADATA_GUARD is not set" /tmp/control-default-off.config
COPY profiles/normal-split-metadata-v1/linux-config-amd64-6.12.26 /build/linux-6.12.26/.config
ARG CONTROL_METADATA_VERSION_SUFFIX
RUN --network=none cd /build/linux-6.12.26 && make ARCH=x86_64 CROSS_COMPILE=x86_64-linux-gnu- EXTRAVERSION="$CONTROL_METADATA_VERSION_SUFFIX" -j 4
FROM control_metadata_build AS control_metadata_receipt
RUN --network=none set -eu; cd /build/linux-6.12.26; mkdir /control-metadata-receipt; cp .config /control-metadata-receipt/kernel.config; cp /tmp/control-default-off.config /control-metadata-receipt/default-off.config; cp include/config/kernel.release /control-metadata-receipt/kernel.release; cp init/Kconfig /control-metadata-receipt/Kconfig.source; cp fs/kernfs/inode.c /control-metadata-receipt/kernfs-inode.source.c; objcopy --dump-section .notes=/control-metadata-receipt/kernel-notes.bin vmlinux; sha256sum /build/linux-6.12.26.tar.xz init/Kconfig fs/kernfs/inode.c mm/page_counter.c security/landlock/task.c include/uapi/linux/landlock.h /build/patches/*.patch .config arch/x86/boot/bzImage >/control-metadata-receipt/actual-source-SHA256SUMS; objdump -d --disassemble=kernfs_iop_setattr vmlinux >/control-metadata-receipt/kernfs-setattr.asm; objdump -d --disassemble=kernfs_vfs_xattr_set vmlinux >/control-metadata-receipt/kernfs-xattr.asm; objdump -d --disassemble=kernfs_vfs_user_xattr_set vmlinux >/control-metadata-receipt/kernfs-user-xattr.asm; cat /sys/fs/cgroup/memory.max >/control-metadata-receipt/memory.max; cat /sys/fs/cgroup/cpu.max >/control-metadata-receipt/cpu.max
FROM scratch AS control_metadata_export
COPY --from=control_metadata_receipt /build/linux-6.12.26/arch/x86/boot/bzImage /vmlinuz
COPY --from=control_metadata_receipt /control-metadata-receipt/ /

FROM control_metadata_build AS force_source_review
COPY force-source-audit.py /force-source-audit.py
RUN --network=none python3 /force-source-audit.py
FROM scratch AS force_source_export
COPY --from=force_source_review /force-review/ /

FROM force_source_review AS force_extra_review
COPY force-extra-audit.py /force-extra-audit.py
RUN --network=none python3 /force-extra-audit.py
FROM scratch AS force_extra_export
COPY --from=force_extra_review /force-extra/ /

FROM force_extra_review AS force_q1_review
COPY force-q1-audit.py /force-q1-audit.py
RUN --network=none python3 /force-q1-audit.py
FROM scratch AS force_q1_export
COPY --from=force_q1_review /force-q1/ /

FROM force_q1_review AS force_leaf_review
COPY force-leaf-audit.py /force-leaf-audit.py
RUN --network=none python3 /force-leaf-audit.py
FROM scratch AS force_leaf_export
COPY --from=force_leaf_review /force-leaf/ /

FROM force_leaf_review AS force_fs_leaf_review
COPY force-fs-leaf-audit.py /force-fs-leaf-audit.py
RUN --network=none python3 /force-fs-leaf-audit.py
FROM scratch AS force_fs_leaf_export
COPY --from=force_fs_leaf_review /force-fs-leaf/ /
