#!/usr/bin/env python3
"""校验候选发行的不可变来源、依赖闭包与实际 Guest kernel 收据。"""

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
SHA = re.compile(r"[a-f0-9]{40}\Z")
DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")
HASH = re.compile(r"[a-f0-9]{64}\Z")
TAG = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\Z")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path):
    return json.loads(pathlib.Path(path).read_text(), object_pairs_hook=unique_object)


LOCK = read_json(ROOT / "ci/distribution.json")


def sha256(path):
    digest = hashlib.sha256()
    with pathlib.Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_value(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()


def validate_arch(arch):
    if arch not in {"amd64", "arm64"}:
        raise ValueError("Only native amd64 and arm64 builds are supported")


def validate_tag(tag):
    if not TAG.fullmatch(tag) or tag.lower() in {"latest", "stable", "main", "edge", "nightly"}:
        raise ValueError("An immutable release version is required; moving aliases are forbidden")


def execution_identity(environ=None):
    env = os.environ if environ is None else environ
    repository = env.get("GITHUB_REPOSITORY", "")
    if repository not in {"william-lbn/autoscaling", "william-lbn/neon"}:
        raise ValueError("The distribution must run in an owned source or orchestrator repository")
    run_id, attempt = env.get("GITHUB_RUN_ID", ""), env.get("GITHUB_RUN_ATTEMPT", "")
    if not re.fullmatch(r"[1-9][0-9]*", run_id) or not re.fullmatch(r"[1-9][0-9]*", attempt):
        raise ValueError("Missing GitHub run identity")
    return {
        "repository": repository,
        "run_id": run_id,
        "run_attempt": attempt,
        "run_url": f"https://github.com/{repository}/actions/runs/{run_id}/attempts/{attempt}",
    }


def validate_attempt(tag, arch, execution):
    """失败任务的单独重跑不能覆盖上一 attempt 的镜像；必须重跑完整发行。"""
    validate_arch(arch)
    validate_tag(tag)
    candidate = f"0.2.0-dsga.{execution['run_id']}.{execution['run_attempt']}.{arch}"
    # 保持原 main 日期版本合同；新 DSGA 统一版本使用上面的 SemVer 预发行号。
    legacy = rf"[0-9]{{4}}\.[0-9]{{2}}\.[0-9]{{2}}-[0-9]{{6}}-[a-f0-9]{{12}}-r{execution['run_id']}-a{execution['run_attempt']}"
    if tag != candidate and not re.fullmatch(legacy, tag):
        raise ValueError("Release version does not belong to this run/attempt/architecture; rerun all jobs")


def verify_source(source):
    if not SHA.fullmatch(source) or git_value("rev-parse", "HEAD") != source:
        raise ValueError("Checkout is not the exact requested source SHA")


def validate_record(item, source, version, arch, execution):
    validate_arch(arch)
    validate_tag(version)
    name = item.get("name")
    if name not in LOCK["images"]:
        raise ValueError(f"Unexpected image: {name}")
    expected = {
        "kind": "runtime",
        "source_repository": LOCK["source_repository"],
        "source_commit": source,
        "architecture": "linux/" + arch,
        "image": f"{LOCK['registry']}/{name}:{version}",
        "execution": execution,
    }
    for key, value in expected.items():
        if item.get(key) != value:
            raise ValueError(f"Image {name} has a different {key}")
    if not DIGEST.fullmatch(item.get("digest", "")):
        raise ValueError(f"Image {name} has an invalid digest")
    dependencies = item.get("dependencies")
    if not isinstance(dependencies, dict) or set(dependencies) != set(LOCK["images"][name]):
        raise ValueError(f"Image {name} has an incomplete dependency closure")
    if any(not DIGEST.fullmatch(value) for value in dependencies.values()):
        raise ValueError(f"Image {name} has an unpinned dependency")


def load_records(directory, source, version, arch, execution):
    records = {}
    for path in sorted(pathlib.Path(directory).rglob("*.json")):
        item = read_json(path)
        if not isinstance(item, dict) or item.get("kind") != "runtime":
            continue
        validate_record(item, source, version, arch, execution)
        name = item["name"]
        # 即使两份字节相同也拒绝重复，避免错误合并下载目录掩盖来源混用。
        if name in records:
            raise ValueError(f"Duplicate image record: {name}")
        records[name] = item
    if set(records) != set(LOCK["images"]):
        raise ValueError(f"Incomplete distribution: missing={sorted(set(LOCK['images']) - set(records))}")
    for name, item in records.items():
        for dependency, digest in item["dependencies"].items():
            if digest != records[dependency]["digest"]:
                raise ValueError(f"Image {name} did not consume this release's {dependency}")
    return records


def expected_audited_identity():
    """历史已审五元组仅用于说明差异；本函数绝不向 resourceguard 注册新构建。"""
    profile = read_json(ROOT / LOCK["audited_kernel_profile"])
    compiled = profile["compiled_tuple"]
    identity = {"profile_id": profile["physical_profile_id"], "kernel_release": compiled["release"]}
    for key in ["artifact", "config", "notes"]:
        value = compiled[key + "_sha256"]
        if not HASH.fullmatch(value):
            raise ValueError("Audited kernel identity is incomplete")
        identity["kernel_" + key + "_sha256"] = value
    # 收据与运行中白名单必须仍为同一份已审身份；不可由 CI 自动改 Go 常量。
    reviewed_code = (ROOT / "pkg/dsga/resourceguard/physical_kernel_build.go").read_text()
    if any('"' + value + '"' not in reviewed_code for value in identity.values()):
        raise ValueError("Audited kernel receipt differs from the compiled resourceguard allowlist")
    return identity


def kernel_receipt(directory, arch):
    validate_arch(arch)
    directory = pathlib.Path(directory)
    filenames = ["vmlinuz", "kernel.config", "kernel-notes.bin", "kernel.release"]
    for name in filenames:
        path = directory / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"Missing actual kernel receipt: {name}")
    kernel_version = subprocess.check_output([str(ROOT / "neonvm-kernel/echo-version.sh")], text=True).strip()
    tree = git_value("rev-parse", "HEAD:neonvm-kernel")
    release = (directory / "kernel.release").read_text().strip()
    if release != kernel_version + "-william-" + tree[:12]:
        raise ValueError("Kernel release does not match this source tree")
    config = (directory / "kernel.config").read_text()
    # 默认发行没有应用 metadata 私有 patch；若以后修改构建选择，必须显式升级合同。
    if any(line in config.splitlines() for line in [
        "CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER=y", "CONFIG_NEON_DSGA_CONTROL_METADATA_GUARD=y"
    ]):
        raise ValueError("Kernel capabilities changed without updating the candidate distribution contract")
    config_arch = "amd64" if arch == "amd64" else "aarch64"
    input_config = ROOT / f"neonvm-kernel/linux-config-{config_arch}-{kernel_version}"
    return {
        "profile": LOCK["kernel_build_profile"],
        "release": release,
        "artifact_sha256": sha256(directory / "vmlinuz"),
        "config_sha256": sha256(directory / "kernel.config"),
        "notes_sha256": sha256(directory / "kernel-notes.bin"),
        "input_config_sha256": sha256(input_config),
        "source_tree": tree,
        "patches": {path.name: sha256(path) for path in sorted((ROOT / "neonvm-kernel/patches").glob("*.patch"))},
        "normal_metadata_guard": False,
        "normal_qualification": False,
    }


def collect(directory, kernel_directory, source, version, arch, execution):
    verify_source(source)
    validate_attempt(version, arch, execution)
    records = load_records(directory, source, version, arch, execution)
    observed_kernel = kernel_receipt(kernel_directory, arch)
    observed_kernel["image"] = records["vm-kernel"]["image"] + "@" + records["vm-kernel"]["digest"]
    # 三份原始文件由 kernel job 从其 digest 拉取的镜像导出，并与 kernel record 一同审计。
    binding = read_json(pathlib.Path(kernel_directory) / "binding.json")
    expected_binding = {"source_commit": source, "architecture": "linux/" + arch,
                        "image_digest": records["vm-kernel"]["digest"], "execution": execution,
                        "kernel": kernel_receipt(kernel_directory, arch)}
    if binding != expected_binding:
        raise ValueError("Kernel receipt is not bound to this release's exact image and run")
    return {
        "kind": "distribution-capabilities",
        "schema_version": LOCK["schema_version"],
        "source_repository": LOCK["source_repository"],
        "source_commit": source,
        "source_workflow_sha256": sha256(ROOT / ".github/workflows/dockerhub.yml"),
        "version": version,
        "architecture": "linux/" + arch,
        "execution": execution,
        "release_channel": LOCK["release_channel"],
        "normal_qualification": False,
        "production_ready": False,
        "latest_eligible": False,
        "vm_builder_default_system_mode": LOCK["vm_builder_default_system_mode"],
        "kernel": observed_kernel,
        "expected_audited_identity": expected_audited_identity(),
        "image_digests": {name: item["digest"] for name, item in sorted(records.items())},
        "requirements_for_normal": [
            "Retrieve and verify the reviewed kernel artifact/config/notes, or separately qualify a new exact build",
            "Build the VM with the normal-split-v3 plan and original ordinary package lock",
            "Record each PG executable digest and the per-instance bootstrap/qualification evidence",
            "Run physical KVM and Helm end-to-end acceptance for this exact image set",
        ],
    }


def write_json(path, value):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    verify = sub.add_parser("verify")
    inspect = sub.add_parser("inspect-kernel")
    aggregate = sub.add_parser("collect")
    for command in [verify, inspect, aggregate]:
        command.add_argument("--source-commit", required=True)
        command.add_argument("--version", required=True)
        command.add_argument("--arch", required=True, choices=["amd64", "arm64"])
    inspect.add_argument("--directory", required=True)
    inspect.add_argument("--digest", required=True)
    inspect.add_argument("--output", required=True)
    aggregate.add_argument("--directory", required=True)
    aggregate.add_argument("--kernel-directory", required=True)
    aggregate.add_argument("--output", required=True)
    args = parser.parse_args()
    identity = execution_identity()
    verify_source(args.source_commit)
    validate_attempt(args.version, args.arch, identity)
    if args.command == "verify":
        return
    if args.command == "inspect-kernel":
        if not DIGEST.fullmatch(args.digest):
            raise ValueError("Invalid kernel image digest")
        write_json(args.output, {"source_commit": args.source_commit,
                                "architecture": "linux/" + args.arch,
                                "image_digest": args.digest, "execution": identity,
                                "kernel": kernel_receipt(args.directory, args.arch)})
        return
    value = collect(args.directory, args.kernel_directory, args.source_commit, args.version, args.arch, identity)
    write_json(args.output, value)
    print(f"Verified {len(value['image_digests'])} immutable image records; candidate, normal_qualification=false")


if __name__ == "__main__":
    main()
