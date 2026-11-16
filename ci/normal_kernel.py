"""独立normal内核的原材料、实际编译与发行读回；不签发live资格。"""

import argparse
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess

import distribution as release


PROFILE = release.ROOT / release.LOCK["audited_kernel_profile"]
FILES = ("vmlinuz", "kernel.config", "kernel-notes.bin", "kernel.release", "actual-source-SHA256SUMS",
         "memory.max", "cpu.max", "compiler.txt", "linker.txt", "toolchain-packages.txt")


def require(ok, message):
    if not ok:
        raise ValueError("Normal kernel " + message)


def profile_builder():
    spec = importlib.util.spec_from_file_location("normal_profile_builder", PROFILE.parent / "build.py")
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def receipt(directory, arch):
    """只认当前归档实读值与原已审四元组全等；拒绝把普通kernel改标签。"""
    require(arch == "amd64", "has no audited arm64 profile")
    root = Path(directory)
    for name in FILES:
        path = root / name
        require(path.is_file() and not path.is_symlink() and path.stat().st_size > 0, "original receipt is missing: " + name)
    desc = release.read_json(PROFILE)
    actual = {"release": (root / "kernel.release").read_text().strip(),
              "artifact_sha256": release.sha256(root / "vmlinuz"),
              "config_sha256": release.sha256(root / "kernel.config"),
              "notes_sha256": release.sha256(root / "kernel-notes.bin")}
    require(actual == desc["compiled_tuple"], "actual compile differs from the audited tuple; qualification cannot be inherited")
    require(release.sha256(root / "actual-source-SHA256SUMS") == desc["actual_build_source_receipt_sha256"],
            "actual source receipt differs from the audited build")
    config = (root / "kernel.config").read_text().splitlines()
    require(all(line in config for line in ("CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER=y", "CONFIG_NEON_DSGA_CONTROL_METADATA_GUARD=y")),
            "metadata/strict counter profile is not enabled")
    require((root / "memory.max").read_text().strip() == "6442450944" and
            (root / "cpu.max").read_text().strip() == "400000 100000", "actual build resource limit differs")
    builder = profile_builder()
    require((root / "compiler.txt").read_text().splitlines()[0] == builder.AUDITED_GCC_VERSION and
            (root / "linker.txt").read_text().splitlines()[0] == builder.AUDITED_LD_VERSION,
            "actual compiler/linker drifted; an audited toolchain input is required")
    return {**actual, "profile": desc["physical_profile_id"], "architecture": "linux/amd64",
            "normal_metadata_guard": True, "normal_qualification": False,
            "profile_source_sha256": release.sha256(PROFILE),
            "actual_source_receipt_sha256": release.sha256(root / "actual-source-SHA256SUMS"),
            "toolchain_sha256": {name: release.sha256(root / name) for name in ("compiler.txt", "linker.txt", "toolchain-packages.txt")}}


def image_record(source, version, digest, identity, observed):
    require(isinstance(digest, str) and release.DIGEST.fullmatch(digest) and digest != "sha256:" + "0" * 64,
            "image digest is missing")
    value = {"kind": "runtime", "name": "vm-kernel-normal",
             "image": release.LOCK["registry"] + "/vm-kernel-normal:" + version, "digest": digest,
             "source_repository": release.LOCK["source_repository"], "source_commit": source,
             "architecture": "linux/amd64", "execution": identity, "dependencies": {}, **observed}
    release.validate_record(value, source, version, "amd64", identity, allow_normal=True)
    return value


def collect_receipt(directory, record, source, version, arch, identity):
    observed = receipt(directory, arch)
    expected_record = image_record(source, version, record["digest"], identity, observed)
    require(record == expected_record, "image record differs from its original readback")
    expected_binding = {"source_commit": source, "architecture": "linux/amd64", "execution": identity,
                        "image_digest": record["digest"], "kernel": observed}
    require(release.read_json(Path(directory) / "binding.json") == expected_binding,
            "original binding belongs to another image/source/run")
    return {**observed, "image": record["image"] + "@" + record["digest"]}


def build(args, identity):
    output = Path(args.output).resolve()
    require(not output.exists(), "output already exists; retain the original attempt instead of overwriting")
    require(not (output.parent / "vm-kernel-normal.json").exists(), "original image record already exists")
    archive = Path(args.source_archive).resolve(strict=True)
    desc = release.read_json(PROFILE)
    require(archive.name == desc["linux_source_archive"]["name"] and
            release.sha256(archive) == desc["linux_source_archive"]["sha256"], "Linux source archive differs")
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", args.builder) is not None, "explicit builder name is invalid")
    help_text = subprocess.check_output(["docker", "buildx", "build", "--help"], text=True)
    require("--resource" in help_text, "Buildx cannot enforce the audited RUN resource limits")
    output.parent.mkdir(parents=True, exist_ok=True)
    # 既有profile自己保留失败日志、实际context/source和命令。没有匹配四元组就不进入发布。
    subprocess.run(["python3", str(PROFILE.parent / "build.py"), "--output", str(output),
                    "--source-archive", str(archive), "--reproduce-audited", "--builder", args.builder,
                    "--network", "default"], check=True)
    observed = receipt(output / "export", "amd64")
    require(release.read_json(output / "result.json").get("audited_tuple_match") is True and
            release.read_json(output / "source-lock.json").get("repo_head") == args.source_commit,
            "profile result does not bind the current source and audited output")
    # 原始字节封装到专用镜像；普通vm-kernel的输入、tag、依赖和能力完全独立。
    context = output / "image-context"
    context.mkdir()
    for name in FILES:
        shutil.copyfile(output / "export" / name, context / name)
    target = release.LOCK["registry"] + "/vm-kernel-normal:" + args.version
    metadata = output / "image-metadata.json"
    command = ["docker", "buildx", "build", "--builder", args.builder, "--platform", "linux/amd64",
               "--file", str(release.ROOT / "ci/normal-kernel.Dockerfile"), "--tag", target,
               "--metadata-file", str(metadata), "--provenance=mode=max", "--sbom=true", "--push"]
    for key, value in {"source": "https://github.com/" + release.LOCK["source_repository"],
                       "revision": args.source_commit, "version": args.version}.items():
        command.extend(["--label", "org.opencontainers.image." + key + "=" + value])
    command.append(str(context))
    release.write_json(output / "publication-intent.json", {"source_commit": args.source_commit, "execution": identity,
        "image": target, "kernel": observed, "argv": command, "normal_qualification": False})
    subprocess.run(command, check=True)
    digest = release.read_json(metadata)["containerimage.digest"]
    # 即使随后拉取失败也保留发布摘要；重跑整个工作流会得到新attempt，不能原地改写旧输出。
    release.write_json(output / "published.json", {"image": target, "digest": digest, "execution": identity})
    read_image(args, identity, target, digest, output)


def read_image(args, identity, image, digest, output):
    require(release.DIGEST.fullmatch(digest) is not None, "published OCI metadata lacks digest")
    reference = image + "@" + digest
    subprocess.run(["docker", "pull", "--platform", "linux/amd64", reference], check=True)
    image_state = json.loads(subprocess.check_output(["docker", "image", "inspect", reference, "--format", "{{json .}}"], text=True))
    labels = image_state.get("Config", {}).get("Labels") or {}
    require((image_state.get("Os"), image_state.get("Architecture")) == ("linux", "amd64") and
            labels.get("org.opencontainers.image.source") == "https://github.com/" + release.LOCK["source_repository"] and
            labels.get("org.opencontainers.image.revision") == args.source_commit and
            labels.get("org.opencontainers.image.version") == args.version,
            "published image platform or source labels differ")
    container = subprocess.check_output(["docker", "create", "--platform", "linux/amd64", "--entrypoint",
                                         "/__normal_kernel_readback_never_run__", reference], text=True).strip()
    require(re.fullmatch(r"[a-f0-9]{64}", container) is not None, "original readback container is missing")
    directory = output / "readback"
    try:
        directory.mkdir()
        for final in (False, True):
            state = json.loads(subprocess.check_output(["docker", "container", "inspect", container, "--format", "{{json .}}"], text=True))
            require(state.get("Id") == container and state.get("Image") == image_state["Id"] and
                    state.get("Config", {}).get("Image") == reference and state.get("State", {}).get("Status") == "created" and
                    state.get("State", {}).get("Running") is False, "original container identity or stopped state changed")
            if not final:
                for name in FILES:
                    path = "/vmlinuz" if name == "vmlinuz" else "/kernel-receipt/" + name
                    subprocess.run(["docker", "cp", container + ":" + path, str(directory / name)], check=True)
    finally:
        subprocess.run(["docker", "rm", container], check=True)
    observed = receipt(directory, "amd64")
    record = image_record(args.source_commit, args.version, digest, identity, observed)
    release.write_json(directory / "binding.json", {"source_commit": args.source_commit, "architecture": "linux/amd64",
                       "execution": identity, "image_digest": digest, "kernel": observed})
    release.write_json(output.parent / "vm-kernel-normal.json", record)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["build"])
    for name in ("source-commit", "version", "builder", "source-archive", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    identity = release.execution_identity()
    release.verify_source(args.source_commit)
    subprocess.run(["git", "-C", str(release.ROOT), "ls-files", "--error-unmatch",
                    "ci/normal_kernel.py", "ci/normal-kernel.Dockerfile"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(release.ROOT), "diff", "--quiet", args.source_commit, "--",
                    "ci", "neonvm-kernel/profiles"], check=True)
    release.validate_attempt(args.version, "amd64", identity)
    require(args.version == f"0.2.0-dsga.{identity['run_id']}.{identity['run_attempt']}.amd64",
            "requires an explicit immutable DSGA candidate version")
    build(args, identity)


if __name__ == "__main__":
    main()
