"""不可变镜像读回的离线正负控；合成 ELF 仅用于合同测试，不冒充镜像实测。"""

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import distribution
import record
import runtime_elf


def fixture(name="neonvm-daemon", arch="amd64"):
    execution = distribution.execution_identity({"GITHUB_REPOSITORY": "william-lbn/neon",
        "GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2"})
    return {"kind": "runtime", "name": name,
        "image": f"{distribution.LOCK['registry']}/{name}:0.2.0-dsga.1234.2.{arch}",
        "digest": "sha256:" + "1" * 64, "source_repository": "william-lbn/autoscaling",
        "source_commit": "a" * 40, "architecture": "linux/" + arch, "execution": execution,
        "dependencies": {key: "sha256:" + "2" * 64 for key in distribution.LOCK["images"][name]}}


def fake_elf(arch):
    header = bytearray(64)
    header[:7] = b"\x7fELF\x02\x01\x01"
    header[16:18] = (2).to_bytes(2, "little")
    header[18:20] = (62 if arch == "amd64" else 183).to_bytes(2, "little")
    header[20:24] = (1).to_bytes(4, "little")
    header[52:54] = (64).to_bytes(2, "little")
    return bytes(header) + b"TEST-ONLY offline ELF header fixture"


class FakeDocker:
    def __init__(self, item):
        self.item = item
        self.reference = item["image"] + "@" + item["digest"]
        self.id = "b" * 64
        self.image = {"Id": "sha256:" + "3" * 64, "Os": "linux", "Architecture": item["architecture"].split("/")[1],
            "RepoDigests": [item["image"].rsplit(":", 1)[0].removeprefix("docker.io/") + "@" + item["digest"]],
            "Config": {"Labels": {"org.opencontainers.image.source": "https://github.com/" + item["source_repository"],
                "org.opencontainers.image.revision": item["source_commit"],
                "org.opencontainers.image.version": item["image"].rsplit(":", 1)[1]}}}
        self.container = {"Id": self.id, "Image": self.image["Id"], "Config": {"Image": self.reference},
                          "State": {"Status": "created", "Running": False}}
        self.content = fake_elf(self.image["Architecture"])
        self.mode = 0o755
        self.calls = []
        self.fail = None
        self.missing_path = None
        self.symlink = False
        self.drift = False

    def run(self, argv, **kwargs):
        self.calls.append(argv)
        if self.fail == argv[1]:
            raise subprocess.CalledProcessError(1, argv, stderr="TEST-ONLY failure")
        if argv[1:3] == ["image", "inspect"]:
            return SimpleNamespace(stdout=json.dumps(self.image))
        if argv[1] == "create":
            return SimpleNamespace(stdout=self.id + "\n")
        if argv[1:3] == ["container", "inspect"]:
            value = copy.deepcopy(self.container)
            if self.drift and sum(c[1:3] == ["container", "inspect"] for c in self.calls) > 1:
                value["Image"] = "sha256:" + "4" * 64
            return SimpleNamespace(stdout=json.dumps(value))
        if argv[1] == "cp":
            path = Path(argv[-1])
            source = argv[-2].split(":", 1)[1]
            if self.missing_path == source:
                raise subprocess.CalledProcessError(1, argv, stderr="TEST-ONLY missing original executable")
            if self.symlink:
                path.symlink_to("missing")
            else:
                path.write_bytes(self.content + source.encode())
                path.chmod(self.mode)
        return SimpleNamespace(stdout="")


class RuntimeELFTest(unittest.TestCase):
    def test_actual_bytes_both_images_and_both_native_platforms(self):
        for arch in ("amd64", "arm64"):
            for name in runtime_elf.PATHS:
                with self.subTest(arch=arch, name=name):
                    item = fixture(name, arch)
                    docker = FakeDocker(item)
                    result = runtime_elf.inspect_runtime_elf(item, docker.run)
                    digests = {key: hashlib.sha256(docker.content + source.encode()).hexdigest()
                               for key, source in runtime_elf.PATHS[name].items()}
                    if name == "vm-builder":
                        self.assertEqual(result["builder_sha256"], digests["vm_builder"])
                    else:
                        self.assertEqual(result["runtime_elf_sha256"], digests)
                        self.assertNotEqual(digests["neonvmd"], digests["dsga_resource_guard"])
                    proof = result["runtime_elf_readback"]
                    self.assertEqual(proof["image"], docker.reference)
                    self.assertEqual(proof["execution"], item["execution"])
                    self.assertEqual(proof["image_config_digest"], docker.image["Id"])
                    self.assertEqual({k: v["path"] for k, v in proof["files"].items()}, runtime_elf.PATHS[name])
                    self.assertEqual(docker.calls[0], ["docker", "pull", "--platform", "linux/" + arch, docker.reference])
                    self.assertEqual(docker.calls[-1], ["docker", "rm", docker.id])
                    self.assertFalse(any(c[1] in ("run", "start", "exec", "push") for c in docker.calls))

    def test_wrong_image_identity_rejected_before_creating_container(self):
        changes = [lambda d: d.image.update(Architecture="arm64"),
                   lambda d: d.image.update(RepoDigests=["foreign/image@" + d.item["digest"]]),
                   lambda d: d.image.update(Id="not-an-image-config"),
                   lambda d: d.image["Config"]["Labels"].update({"org.opencontainers.image.revision": "f" * 40}),
                   lambda d: d.image["Config"]["Labels"].update({"org.opencontainers.image.version": "0.2.0-dsga.1234.1.amd64"})]
        for change in changes:
            docker = FakeDocker(fixture()); change(docker)
            with self.assertRaises(ValueError): runtime_elf.inspect_runtime_elf(docker.item, docker.run)
            self.assertFalse(any(c[1] == "create" for c in docker.calls))

    def test_wrong_original_container_or_started_state_rejected_and_removed(self):
        changes = [lambda d: d.container.update(Id="c" * 64),
                   lambda d: d.container.update(Image="sha256:" + "f" * 64),
                   lambda d: d.container["Config"].update(Image="tag-only"),
                   lambda d: d.container["State"].update(Status="running", Running=True),
                   lambda d: setattr(d, "drift", True)]
        for change in changes:
            docker = FakeDocker(fixture()); change(docker)
            with self.assertRaises(ValueError): runtime_elf.inspect_runtime_elf(docker.item, docker.run)
            self.assertEqual(docker.calls[-1], ["docker", "rm", docker.id])

    def test_missing_or_non_elf_or_wrong_arch_or_link_fails_closed(self):
        changes = [lambda d: setattr(d, "fail", "cp"), lambda d: setattr(d, "content", b"not an ELF"),
                   lambda d: setattr(d, "content", fake_elf("arm64")), lambda d: setattr(d, "mode", 0o644),
                   lambda d: setattr(d, "symlink", True)]
        for change in changes:
            docker = FakeDocker(fixture()); change(docker)
            with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                runtime_elf.inspect_runtime_elf(docker.item, docker.run)
            self.assertEqual(docker.calls[-1], ["docker", "rm", docker.id])

    def test_read_or_cleanup_error_cannot_emit_success(self):
        for failure in ("pull", "image", "create", "container", "rm"):
            docker = FakeDocker(fixture()); docker.fail = failure
            with self.assertRaises(subprocess.CalledProcessError): runtime_elf.inspect_runtime_elf(docker.item, docker.run)

    def test_daemon_without_original_guard_cannot_emit_partial_readback(self):
        docker = FakeDocker(fixture()); docker.missing_path = "/dsga-resource-guard"
        with self.assertRaises(subprocess.CalledProcessError): runtime_elf.inspect_runtime_elf(docker.item, docker.run)
        self.assertEqual(sum(c[1] == "cp" for c in docker.calls), 2)
        self.assertEqual(docker.calls[-1], ["docker", "rm", docker.id])

    def test_existing_timestamp_release_is_read_without_new_version_restrictions(self):
        item = fixture("vm-builder", "arm64")
        item["image"] = item["image"].rsplit(":", 1)[0] + ":2026.11.16-090000-aaaaaaaaaaaa-r1234-a2"
        docker = FakeDocker(item)
        self.assertIn("builder_sha256", runtime_elf.inspect_runtime_elf(item, docker.run))

    def test_other_images_cannot_receive_fabricated_runtime_fields(self):
        item = fixture("vm-kernel")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            runtime_elf.inspect_runtime_elf(item, mock.Mock(side_effect=AssertionError("No Docker call expected")))

    def test_record_cli_writes_only_after_original_elf_success(self):
        item = fixture("vm-builder")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "record.json"
            argv = ["record.py", "--name", item["name"], "--image", item["image"], "--digest", item["digest"],
                    "--source-commit", item["source_commit"], "--arch", "amd64", "--output", str(output), "--inspect-runtime-elf"]
            for name, digest in item["dependencies"].items(): argv.extend(["--dependency", name + "=" + digest])
            docker = FakeDocker(item)
            env = {"GITHUB_REPOSITORY": "william-lbn/neon", "GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2"}
            def inspect(value):
                self.assertEqual(value, item)
                return runtime_elf.inspect_runtime_elf(value, docker.run)
            with mock.patch.object(sys, "argv", argv), mock.patch.dict(os.environ, env), \
                 mock.patch.object(record, "verify_source"), mock.patch.object(record.subprocess, "check_output", return_value=item["source_commit"]), \
                 mock.patch.object(record, "inspect_runtime_elf", side_effect=inspect):
                record.main()
                result = json.loads(output.read_text())
                self.assertEqual(result["builder_sha256"], hashlib.sha256(docker.content + b"/usr/local/bin/vm-builder").hexdigest())
                self.assertEqual(result["dependencies"], item["dependencies"])
                output.unlink(); docker.fail = "cp"
                with self.assertRaises(subprocess.CalledProcessError): record.main()
                self.assertFalse(output.exists())

    def test_official_recipes_and_workflow_use_the_same_elf_paths(self):
        root = distribution.ROOT
        self.assertIn("/usr/local/bin/vm-builder", (root / "ci/vm-builder.Dockerfile").read_text())
        daemon = (root / "neonvm-daemon/Dockerfile").read_text()
        for path in runtime_elf.PATHS["neonvm-daemon"].values(): self.assertIn(" " + path + "\n", daemon)
        workflow = (root / ".github/workflows/dockerhub.yml").read_text()
        self.assertIn('--dependency "autoscaling-go-base=$GO_DIGEST" --inspect-runtime-elf --output output/neonvm-daemon.json', workflow)
        self.assertIn('dependencies+=(--dependency "neonvm-daemon=$DAEMON_DIGEST")\n            readback+=(--inspect-runtime-elf)', workflow)
        self.assertIn('"${dependencies[@]}" "${readback[@]}"', workflow)


if __name__ == "__main__":
    unittest.main()
