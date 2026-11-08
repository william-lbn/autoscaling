#!/usr/bin/env python3
"""发行拒错测试：缺产物、混合提交/架构/attempt、漂移依赖与伪内核收据必须失败。"""

import copy
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import unittest

import distribution as release


class DistributionContractTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.images = self.root / "images"
        self.kernel = self.root / "kernel"
        self.images.mkdir()
        self.kernel.mkdir()
        self.source = release.git_value("rev-parse", "HEAD")
        self.identity = release.execution_identity({
            "GITHUB_REPOSITORY": "william-lbn/neon",
            "GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2",
        })
        self.version = "0.2.0-dsga.1234.2.amd64"
        self.digests = {name: "sha256:" + hashlib.sha256(name.encode()).hexdigest()
                        for name in release.LOCK["images"]}
        self.records = {}
        for name, dependencies in release.LOCK["images"].items():
            item = {
                "kind": "runtime", "name": name,
                "image": f"{release.LOCK['registry']}/{name}:{self.version}",
                "digest": self.digests[name],
                "source_repository": "william-lbn/autoscaling",
                "source_commit": self.source, "architecture": "linux/amd64",
                "execution": self.identity,
                "dependencies": {dependency: self.digests[dependency] for dependency in dependencies},
            }
            self.records[name] = item
            release.write_json(self.images / (name + ".json"), item)
        # 小型合成收据只用于验证发布检查器，不声称是可启动内核或实际 KVM 结果。
        version = subprocess.check_output([str(release.ROOT / "neonvm-kernel/echo-version.sh")], text=True).strip()
        kernel_release = version + "-william-" + release.git_value("rev-parse", "HEAD:neonvm-kernel")[:12]
        (self.kernel / "vmlinuz").write_bytes(b"TEST-ONLY fake kernel bytes")
        (self.kernel / "kernel-notes.bin").write_bytes(b"TEST-ONLY fake ELF notes")
        (self.kernel / "kernel.config").write_text("# CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER is not set\n")
        (self.kernel / "kernel.release").write_text(kernel_release + "\n")
        self.write_binding()

    def write_binding(self):
        release.write_json(self.kernel / "binding.json", {
            "source_commit": self.source, "architecture": "linux/amd64",
            "image_digest": self.digests["vm-kernel"], "execution": self.identity,
            "kernel": release.kernel_receipt(self.kernel, "amd64"),
        })

    def collect(self):
        return release.collect(self.images, self.kernel, self.source, self.version, "amd64", self.identity)

    def replace_record(self, name, key, value):
        item = copy.deepcopy(self.records[name])
        item[key] = value
        release.write_json(self.images / (name + ".json"), item)

    def test_complete_candidate_keeps_actual_and_audited_kernel_distinct(self):
        result = self.collect()
        self.assertEqual(len(result["image_digests"]), 11)
        self.assertIn("dsga-platform-qualifier", result["image_digests"])
        self.assertEqual(result["release_channel"], "candidate")
        for name in ["normal_qualification", "production_ready", "latest_eligible"]:
            self.assertIs(result[name], False)
        self.assertEqual(result["vm_builder_default_system_mode"], "legacy")
        self.assertEqual(result["kernel"]["profile"], "default-source-build")
        self.assertEqual(result["kernel"]["artifact_sha256"], release.sha256(self.kernel / "vmlinuz"))
        self.assertNotEqual(result["kernel"]["artifact_sha256"],
                            result["expected_audited_identity"]["kernel_artifact_sha256"])

    def test_missing_qualifier_is_incomplete_distribution(self):
        (self.images / "dsga-platform-qualifier.json").unlink()
        with self.assertRaisesRegex(ValueError, "Incomplete distribution"):
            self.collect()

    def test_duplicate_even_identical_record_is_rejected(self):
        release.write_json(self.images / "duplicate.json", self.records["vm-builder"])
        with self.assertRaisesRegex(ValueError, "Duplicate image record"):
            self.collect()

    def test_mixed_source_platform_run_and_registry_are_rejected(self):
        name = "neonvm-runner"
        mutations = {
            "source_commit": "a" * 40,
            "source_repository": "neondatabase/autoscaling",
            "architecture": "linux/arm64",
            "execution": dict(self.identity, run_attempt="1"),
            "image": "docker.io/other/neonvm-runner:" + self.version,
            "digest": "sha256:not-a-digest",
        }
        for key, value in mutations.items():
            with self.subTest(key=key):
                self.replace_record(name, key, value)
                with self.assertRaises(ValueError):
                    self.collect()
                release.write_json(self.images / (name + ".json"), self.records[name])

    def test_builder_must_use_this_daemon_digest(self):
        dependencies = dict(self.records["vm-builder"]["dependencies"], **{"neonvm-daemon": "sha256:" + "a" * 64})
        self.replace_record("vm-builder", "dependencies", dependencies)
        with self.assertRaisesRegex(ValueError, "did not consume this release's neonvm-daemon"):
            self.collect()

    def test_missing_kernel_dependency_is_rejected(self):
        self.replace_record("neonvm-runner", "dependencies", {"autoscaling-go-base": self.digests["autoscaling-go-base"]})
        with self.assertRaisesRegex(ValueError, "incomplete dependency closure"):
            self.collect()

    def test_replaced_kernel_bytes_cannot_inherit_old_binding(self):
        (self.kernel / "vmlinuz").write_bytes(b"different kernel")
        with self.assertRaisesRegex(ValueError, "not bound"):
            self.collect()

    def test_kernel_binding_from_another_image_is_rejected(self):
        binding = release.read_json(self.kernel / "binding.json")
        binding["image_digest"] = "sha256:" + "b" * 64
        release.write_json(self.kernel / "binding.json", binding)
        with self.assertRaisesRegex(ValueError, "not bound"):
            self.collect()

    def test_missing_or_foreign_kernel_receipt_is_rejected(self):
        for name in ["vmlinuz", "kernel.config", "kernel-notes.bin", "kernel.release"]:
            with self.subTest(name=name):
                path = self.kernel / name
                contents = path.read_bytes()
                path.unlink()
                with self.assertRaisesRegex(ValueError, "Missing actual kernel receipt"):
                    self.collect()
                path.write_bytes(contents)
        (self.kernel / "kernel.release").write_text("unreviewed-alias\n")
        with self.assertRaisesRegex(ValueError, "Kernel release does not match"):
            self.collect()

    def test_changed_profile_cannot_silently_claim_default_contract(self):
        (self.kernel / "kernel.config").write_text("CONFIG_NEON_DSGA_CONTROL_METADATA_GUARD=y\n")
        with self.assertRaisesRegex(ValueError, "capabilities changed"):
            self.collect()

    def test_moving_aliases_old_attempt_and_wrong_arch_are_rejected(self):
        for version in ["latest", "stable", "0.2.0-dsga.1234.1.amd64", "0.2.0-dsga.1234.2.arm64", "v1.0.0"]:
            with self.subTest(version=version), self.assertRaises(ValueError):
                release.validate_attempt(version, "amd64", self.identity)

    def test_existing_main_timestamp_contract_is_supported(self):
        release.validate_attempt("2026.11.08-090000-" + self.source[:12] + "-r1234-a2", "amd64", self.identity)

    def test_duplicate_json_key_is_not_last_value_wins(self):
        (self.images / "vm-builder.json").write_text('{"kind":"runtime","kind":"skip"}')
        with self.assertRaisesRegex(ValueError, "Duplicate JSON key"):
            self.collect()

    def test_source_ref_must_be_full_sha_of_the_checkout(self):
        for source in ["main", self.source[:12], "b" * 40]:
            with self.subTest(source=source), self.assertRaises(ValueError):
                release.verify_source(source)

    def test_unowned_execution_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "owned"):
            release.execution_identity({"GITHUB_REPOSITORY": "someone/fork", "GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2"})

    def test_record_cli_uses_caller_run_and_pinned_dependencies(self):
        target = self.root / "record-cli.json"
        env = dict(os.environ, GITHUB_REPOSITORY="william-lbn/neon", GITHUB_RUN_ID="1234",
                   GITHUB_RUN_ATTEMPT="2", ARCH="amd64", RELEASE_SOURCE=self.source)
        command = ["python3", str(release.ROOT / "ci/record.py"), "--name", "vm-builder",
                   "--image", self.records["vm-builder"]["image"], "--digest", self.digests["vm-builder"],
                   "--output", str(target)]
        for name in ["autoscaling-go-base", "neonvm-daemon"]:
            command.extend(["--dependency", name + "=" + self.digests[name]])
        subprocess.run(command, env=env, cwd=release.ROOT, check=True, capture_output=True, text=True)
        self.assertEqual(release.read_json(target), self.records["vm-builder"])

    def test_record_cli_rejects_duplicate_dependency_without_writing(self):
        target = self.root / "invalid-cli.json"
        env = dict(os.environ, GITHUB_REPOSITORY="william-lbn/neon", GITHUB_RUN_ID="1234",
                   GITHUB_RUN_ATTEMPT="2", ARCH="amd64", RELEASE_SOURCE=self.source)
        command = ["python3", str(release.ROOT / "ci/record.py"), "--name", "vm-builder",
                   "--image", self.records["vm-builder"]["image"], "--digest", self.digests["vm-builder"],
                   "--output", str(target), "--dependency", "neonvm-daemon=" + self.digests["neonvm-daemon"],
                   "--dependency", "neonvm-daemon=" + self.digests["neonvm-daemon"]]
        result = subprocess.run(command, env=env, cwd=release.ROOT, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
