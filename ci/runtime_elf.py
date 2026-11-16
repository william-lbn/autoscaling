"""从不可变发行镜像读取实际 ELF；不运行容器，不发放 normal 运行资格。"""

import hashlib
import json
from pathlib import Path
import re
import stat
import subprocess
import tempfile

from distribution import DIGEST, unique_object


PATHS = {
    "vm-builder": {"vm_builder": "/usr/local/bin/vm-builder"},
    "neonvm-daemon": {"neonvmd": "/neonvmd", "dsga_resource_guard": "/dsga-resource-guard"},
}


def require(ok, message):
    if not ok:
        raise ValueError("Runtime ELF " + message)


def canonical_reference(value):
    # Docker Hub 的 RepoDigests 可省略默认 registry；只消除这一语法差异。
    for prefix in ("docker.io/", "index.docker.io/"):
        if value.startswith(prefix):
            return value[len(prefix):]
    return value


def elf_file(path, architecture):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_size >= 64 and info.st_mode & 0o111,
            "artifact is not a regular executable file")
    with path.open("rb") as stream:
        header = stream.read(64)
        # 两个正式平台均是 ELF64 little-endian；不把脚本、链接或别的平台字节记为 ELF。
        require(header[:7] == b"\x7fELF\x02\x01\x01" and
                int.from_bytes(header[16:18], "little") in (2, 3) and
                int.from_bytes(header[18:20], "little") == {"linux/amd64": 62, "linux/arm64": 183}[architecture] and
                int.from_bytes(header[20:24], "little") == 1 and int.from_bytes(header[52:54], "little") == 64,
                "artifact is not an executable ELF for the original platform")
        stream.seek(0)
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"sha256": digest, "bytes": info.st_size}


def inspect_runtime_elf(record, run=None):
    """调用方先验原 source/run/dependencies；这里独立核原镜像平台、标签及实际文件。

    只按 digest 拉取并创建 stopped 容器，绝不运行待提取的 ELF。所有文件从这个
    原容器 UID 复制；前后检查容器仍为 created 且绑定原 image config。任何缺件、
    权限、解析或清理错误均向上失败，record 只能在全量读回成功后写出。
    """
    run = subprocess.run if run is None else run
    name, architecture = record.get("name"), record.get("architecture")
    require(name in PATHS and architecture in ("linux/amd64", "linux/arm64"), "unsupported image or platform")
    require(isinstance(record.get("image"), str) and isinstance(record.get("digest"), str) and
            DIGEST.fullmatch(record["digest"]) and record["digest"] != "sha256:" + "0" * 64,
            "requires an immutable source image")
    reference = record["image"] + "@" + record["digest"]
    expected_repository = record["image"].rsplit(":", 1)[0]

    def command(*args):
        return run(["docker", *args], check=True, capture_output=True, text=True, timeout=300).stdout

    def read(*args):
        value = json.loads(command(*args), object_pairs_hook=unique_object)
        require(isinstance(value, dict), "Docker readback is not an object")
        return value

    command("pull", "--platform", architecture, reference)
    image = read("image", "inspect", reference, "--format", "{{json .}}")
    require((image.get("Os"), image.get("Architecture")) == tuple(architecture.split("/")),
            "image platform differs from the record")
    config_id = image.get("Id")
    require(isinstance(config_id, str) and DIGEST.fullmatch(config_id) and config_id != "sha256:" + "0" * 64,
            "image config digest is missing")
    original = canonical_reference(expected_repository + "@" + record["digest"])
    repo_digests = image.get("RepoDigests")
    require(isinstance(repo_digests, list) and any(isinstance(value, str) and canonical_reference(value) == original
            for value in repo_digests), "image no longer binds the requested repository and digest")
    require(isinstance(image.get("Config"), dict), "image configuration is missing")
    labels = image["Config"].get("Labels")
    expected_labels = {"org.opencontainers.image.source": "https://github.com/" + record["source_repository"],
                       "org.opencontainers.image.revision": record["source_commit"],
                       "org.opencontainers.image.version": record["image"].rsplit(":", 1)[1]}
    require(isinstance(labels, dict) and all(labels.get(k) == v for k, v in expected_labels.items()),
            "image source or version labels differ from the original record")
    container = command("create", "--platform", architecture, "--entrypoint", "/__neon_ci_readback_never_run__", reference).strip()
    require(re.fullmatch(r"[a-f0-9]{64}", container) is not None, "container identity is missing")

    def check_container():
        actual = read("container", "inspect", container, "--format", "{{json .}}")
        require(isinstance(actual.get("Config"), dict) and isinstance(actual.get("State"), dict),
                "container configuration or state is missing")
        require(actual.get("Id") == container and actual.get("Image") == config_id and
                actual.get("Config", {}).get("Image") == reference and
                actual.get("State", {}).get("Status") == "created" and actual.get("State", {}).get("Running") is False,
                "container identity, original image or stopped state changed")

    files = {}
    try:
        check_container()
        with tempfile.TemporaryDirectory(prefix="runtime-elf-") as directory:
            for key, source in PATHS[name].items():
                target = Path(directory) / key
                command("cp", container + ":" + source, str(target))
                files[key] = {"path": source, **elf_file(target, architecture)}
        check_container()
    finally:
        command("rm", container)
    # 保留精确提取身份；下游真正使用的摘要均来自上面的原文件字节，不接收手填值。
    fields = ({"builder_sha256": files["vm_builder"]["sha256"]} if name == "vm-builder" else
              {"runtime_elf_sha256": {key: value["sha256"] for key, value in files.items()}})
    fields["runtime_elf_readback"] = {
        "kind": "runtime-image-elf-readback", "protocol_version": 1,
        "image": reference, "image_config_digest": config_id, "architecture": architecture,
        "source_repository": record["source_repository"], "source_commit": record["source_commit"],
        "execution": record["execution"], "files": files,
    }
    return fields
