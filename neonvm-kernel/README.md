# NeonVM kernel

We build a custom kernel in order to:

1. Have support for unusual features that are only required for VMs (e.g. hotplugging, kvm-ptp clock
   synchronization, etc.)
2. Avoid including features we don't need

Kernel images are all at `neondatabase/vm-kernel:$tag`, built by the
[vm-kernel](../.github/workflows/vm-kernel.yaml) github workflow.

Kernel images are typically assigned to a VM based on what was bundled in with the `neonvm-runner`
in use, although this can be overridden on an individual VM basis using the
`.spec.guest.kernelImage` field.

## Upgrading the kernel

Assuming a plain upgrade (i.e. no additional features to enable), upgrading the kernel can be done
with the following sequence of actions:

After copying your old `.config` into the new kernel directory, you can:

```sh
# Quick upgrade using automatic defaults for new config options (non-interactive)
make olddefconfig ARCH=x86_64  # or ARCH=arm64

# OR

# Interactively review all new config options 
make oldconfig ARCH=x86_64  # or ARCH=arm64
```

### On amd64 (x64)

1. On the host, run:
   ```sh
   cd neonvm-kernel # this directory
   docker build --build-arg KERNEL_VERSION=$NEW_VERSION --target build-deps -t kernel-build-deps .
   docker run --rm -v $PWD:/host --name kernel-build -it kernel-build-deps bash
   ```
2. Then, inside the container, run:
   ```sh
   cd linux-$NEW_VERSION
   cp /host/linux-config-amd64-6.6.64 .config # Copy current config in
   make menuconfig ARCH=x86_64
   # do nothing; just save and exit, overwriting .config
   cp .config /host/linux-config-amd64-$NEW_VERSION # NOTE: Different from existing!
   ```
3. Back on the host, finish with:
   ```sh
   # compare the two versions
   diff linux-config-amd64-6.6.64 linux-config-amd64-$NEW_VERSION
   # If all looks good, delete the old version. This is required so auto-selection works.
   rm linux-config-amd64-6.6.64
   ```

### On arm64 (aarch64 ARM)

1. On the host, run:
   ```sh
   cd neonvm-kernel # this directory
   docker build --build-arg KERNEL_VERSION=$NEW_VERSION --target build-deps -t kernel-build-deps .
   docker run --rm -v $PWD:/host --name kernel-build -it kernel-build-deps bash
   ```
2. Then, inside the container, run:
   ```sh
   cd linux-$NEW_VERSION
   cp /host/linux-config-aarch64-6.6.64 .config # Copy current config in
   make menuconfig ARCH=arm64
   # do nothing; just save and exit, overwriting .config
   cp .config /host/linux-config-aarch64-$NEW_VERSION # NOTE: Different from existing!
   ```
3. Back on the host, finish with:
   ```sh
   # compare the two versions
   diff linux-config-aarch64-6.6.64 linux-config-aarch64-$NEW_VERSION
   # If all looks good, delete the old version. This is required so auto-selection works.
   rm linux-config-aarch64-6.6.64
   ```

Afterwards, it's probably also good to do a search-and-replace repo-wide to update all places that
mention the old kernel version.

## Adjusting the config

To adjust the kernel config, try the following from this directory:

```sh
docker build --build-arg KERNEL_VERSION=6.6.64 --platform linux/x86_64 --target build-deps -t kernel-build-deps .
docker run --rm -v $PWD:/host --name kernel-build -it kernel-build-deps bash
# inside that bash shell, do the menuconfig, then copy-out the config to /host
```

## Guest HugeTLB regression probe

Compile `validation/guest-hugetlb-probe.c` on Linux with
`gcc -std=c17 -Wall -Wextra -Werror -Os -static -s`. Package the binary as
`/init` and include a `/dev/console` device in a disposable initramfs. Boot
the candidate kernel in an isolated QEMU Guest with `console=ttyS0 panic=1
hugepagesz=2M hugepages=4 dsga_c6_guest_probe=1 rdinit=/init`. The probe
requires PID 1 and the explicit boot marker before it changes the Guest pool.

Read `DSGA_REMOVE` with the matching `DSGA_POOL` and `DSGA_ALIAS` samples.
In the tested Linux 6.12.26 kernel, a 4 KiB operation zeroed bytes within a
2 MiB huge page while the free-page count held steady. Removing an aligned
2 MiB range increased the free-page count before a later access could fault
the page again. Pool shrink was observed after releasing both mappings and
the backing file. This probe measures the Guest kernel portion of provider
qualification.
