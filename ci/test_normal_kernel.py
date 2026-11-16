"""normal候选发行合同；测试材料均为合成fixture，不是已审内核或live资格。"""

import json
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import distribution
import normal_kernel
import test_distribution


class NormalKernelTest(unittest.TestCase):
    def setUp(self):
        self.base = test_distribution.DistributionContractTest("runTest")
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.directory = self.base.root / "normal"
        self.directory.mkdir()
        content = {"vmlinuz": b"TEST-ONLY kernel", "kernel-notes.bin": b"TEST-ONLY notes",
                   "kernel.config": b"CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER=y\nCONFIG_NEON_DSGA_CONTROL_METADATA_GUARD=y\n",
                   "kernel.release": b"TEST-ONLY release\n", "actual-source-SHA256SUMS": b"TEST-ONLY original source bytes\n",
                   "memory.max": b"6442450944\n", "cpu.max": b"400000 100000\n",
                   "compiler.txt": b"TEST-ONLY gcc\n", "linker.txt": b"TEST-ONLY ld\n",
                   "toolchain-packages.txt": b"TEST-ONLY exact package inventory\n"}
        for name, data in content.items(): (self.directory / name).write_bytes(data)
        self.profile = self.base.root / "profile.json"
        self.desc = {"physical_profile_id": "TEST-ONLY metadata profile", "compiled_tuple": {
            "release": "TEST-ONLY release", "artifact_sha256": distribution.sha256(self.directory / "vmlinuz"),
            "config_sha256": distribution.sha256(self.directory / "kernel.config"),
            "notes_sha256": distribution.sha256(self.directory / "kernel-notes.bin")},
            "actual_build_source_receipt_sha256": distribution.sha256(self.directory / "actual-source-SHA256SUMS")}
        distribution.write_json(self.profile, self.desc)
        self.addCleanup(patch.stopall)
        patch.object(normal_kernel, "PROFILE", self.profile).start()
        patch.object(normal_kernel, "profile_builder", return_value=SimpleNamespace(AUDITED_GCC_VERSION="TEST-ONLY gcc", AUDITED_LD_VERSION="TEST-ONLY ld")).start()
        self.digest = "sha256:" + "9" * 64
        observed = normal_kernel.receipt(self.directory, "amd64")
        self.record = normal_kernel.image_record(self.base.source, self.base.version, self.digest, self.base.identity, observed)
        distribution.write_json(self.base.images / "vm-kernel-normal.json", self.record)
        self.binding = {"source_commit": self.base.source, "architecture": "linux/amd64", "execution": self.base.identity,
                        "image_digest": self.digest, "kernel": observed}
        distribution.write_json(self.directory / "binding.json", self.binding)

    def collect(self):
        return distribution.collect(self.base.images, self.base.kernel, self.base.source, self.base.version, "amd64",
                                    self.base.identity, self.directory)

    def test_explicit_normal_adds_separate_material_without_any_live_qualification(self):
        result = self.collect()
        self.assertEqual(len(result["image_digests"]), 12)
        self.assertFalse(result["kernel"]["normal_metadata_guard"])
        self.assertTrue(result["normal_kernel"]["normal_metadata_guard"])
        self.assertEqual(result["normal_kernel"]["artifact_sha256"], self.record["artifact_sha256"])
        self.assertNotEqual(result["kernel"]["artifact_sha256"], result["normal_kernel"]["artifact_sha256"])
        for field in ("normal_qualification", "production_ready", "latest_eligible"): self.assertIs(result[field], False)
        self.assertIs(result["normal_kernel"]["normal_qualification"], False)
        self.assertEqual(result["vm_builder_default_system_mode"], "legacy")

    def test_normal_is_never_inferred_from_extra_record_or_missing_original_material(self):
        with self.assertRaisesRegex(ValueError, "Unexpected image"):
            self.base.collect()
        (self.base.images / "vm-kernel-normal.json").unlink()
        with self.assertRaisesRegex(ValueError, "Incomplete distribution"):
            self.collect()

    def test_four_actual_values_source_receipt_resources_and_toolchain_fail_closed(self):
        for name in normal_kernel.FILES:
            with self.subTest(name=name):
                path = self.directory / name; original = path.read_bytes()
                path.unlink()
                with self.assertRaisesRegex(ValueError, "receipt is missing"): self.collect()
                path.write_bytes(original)
                if name == "toolchain-packages.txt":
                    # 包清单没有未经证明的历史“期望值”，其实际摘要必须与原binding一致。
                    path.write_bytes(original + b"drift")
                else:
                    path.write_bytes(b"drift")
                with self.assertRaises(ValueError): self.collect()
                path.write_bytes(original)

    def test_binding_must_match_original_source_image_attempt_and_observed_bytes(self):
        for key, value in [("source_commit", "a" * 40), ("image_digest", "sha256:" + "8" * 64),
                           ("execution", dict(self.base.identity, run_attempt="1")), ("kernel", {})]:
            changed = dict(self.binding, **{key: value})
            distribution.write_json(self.directory / "binding.json", changed)
            with self.assertRaisesRegex(ValueError, "binding belongs"): self.collect()
        distribution.write_json(self.directory / "binding.json", self.binding)

    def test_fake_record_qualification_or_kernel_projection_cannot_override_original_bytes(self):
        for key, value in [("normal_qualification", True), ("normal_metadata_guard", False),
                           ("artifact_sha256", "e" * 64), ("profile", "default-source-build")]:
            distribution.write_json(self.base.images / "vm-kernel-normal.json", dict(self.record, **{key: value}))
            with self.assertRaisesRegex(ValueError, "record differs"): self.collect()
        distribution.write_json(self.base.images / "vm-kernel-normal.json", self.record)

    def test_unreviewed_arm64_and_symlink_receipts_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "no audited arm64"): normal_kernel.receipt(self.directory, "arm64")
        path = self.directory / "vmlinuz"; target = self.base.root / "outside"; path.rename(target); path.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "original receipt is missing"): self.collect()

    def test_published_image_is_read_by_original_digest_and_cleanup_precedes_success_record(self):
        output = self.base.root / "published-readback"; output.mkdir()
        reference = self.record["image"] + "@" + self.digest
        container = "c" * 64
        original = {"Id": "sha256:" + "4" * 64, "Os": "linux", "Architecture": "amd64", "Config": {"Labels": {
            "org.opencontainers.image.source": "https://github.com/william-lbn/autoscaling",
            "org.opencontainers.image.revision": self.base.source, "org.opencontainers.image.version": self.base.version}}}
        state = {"Id": container, "Image": original["Id"], "Config": {"Image": reference}, "State": {"Status": "created", "Running": False}}
        args = SimpleNamespace(source_commit=self.base.source, version=self.base.version)
        commands = []
        def read(argv, **kwargs):
            commands.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                self.assertEqual(argv[3], reference); return json.dumps(original)
            if argv[1] == "create":
                self.assertEqual(argv[-1], reference); return container
            if argv[1:3] == ["container", "inspect"]:
                self.assertEqual(argv[3], container); return json.dumps(state)
            raise AssertionError(argv)
        def execute(argv, **kwargs):
            commands.append(argv)
            if argv[1] == "cp":
                name = argv[2].rsplit("/", 1)[1]
                self.assertTrue(argv[2].startswith(container + ":/"))
                shutil.copyfile(self.directory / name, argv[-1])
        with patch.object(normal_kernel.subprocess, "run", side_effect=execute), patch.object(normal_kernel.subprocess, "check_output", side_effect=read):
            normal_kernel.read_image(args, self.base.identity, self.record["image"], self.digest, output)
        self.assertEqual(commands[-1], ["docker", "rm", container])
        self.assertEqual(distribution.read_json(output.parent / "vm-kernel-normal.json"), self.record)
        self.assertEqual(distribution.read_json(output / "readback/binding.json"), self.binding)
        self.assertFalse(any(command[1] in ("start", "run", "exec") for command in commands))
        # 第二次独立fixture模拟cp失败，不能遗留成功记录或遗漏原容器清理。
        (output.parent / "vm-kernel-normal.json").unlink()
        shutil.rmtree(output / "readback")
        def fail_copy(argv, **kwargs):
            if argv[1] == "cp": raise subprocess.CalledProcessError(1, argv)
            commands.append(argv)
        with patch.object(normal_kernel.subprocess, "run", side_effect=fail_copy), patch.object(normal_kernel.subprocess, "check_output", side_effect=read):
            with self.assertRaises(subprocess.CalledProcessError):
                normal_kernel.read_image(args, self.base.identity, self.record["image"], self.digest, output)
        self.assertEqual(commands[-1], ["docker", "rm", container])
        self.assertFalse((output.parent / "vm-kernel-normal.json").exists())


class NormalKernelWiringTest(unittest.TestCase):
    def test_profile_keeps_audited_resources_and_checks_compiler_before_kernel_build(self):
        profile = normal_kernel.profile_builder()
        self.assertEqual(profile.AUDITED_GCC_VERSION, "x86_64-linux-gnu-gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0")
        source = (normal_kernel.PROFILE.parent / "build.py").read_text()
        self.assertIn("'--builder',args.builder", source)
        for fragment in ("'memory=6g'", "'cpu-quota=400000'", "FROM audited_build_deps AS build_amd64", "toolchain-packages.txt"):
            self.assertIn(fragment, source)
        self.assertIn("if args.network == 'host'", source)

    def test_workflow_default_is_off_and_source_archive_is_checked_before_build(self):
        source = (distribution.ROOT / ".github/workflows/dockerhub.yml").read_text()
        self.assertEqual(source.count("      normal_kernel:\n"), 2)
        job = source.split("  normal-kernel:\n", 1)[1].split("  daemon:\n", 1)[0]
        for fragment in ("if: inputs.normal_kernel", "fetch-depth: 0", "memory=6g", "cpu-quota=400000",
                         "version: v0.38.1", "if: always()", "output/normal-build/*.json", "steps.normal-builder.outputs.name"):
            self.assertIn(fragment, job)
        self.assertLess(job.index("hashlib.file_digest"), job.index("python3 ci/normal_kernel.py build"))
        self.assertIn("normal+=(--normal-kernel-directory artifacts/normal-kernel)", source)
        dockerfile = (distribution.ROOT / "ci/normal-kernel.Dockerfile").read_text()
        self.assertNotIn("vm-kernel:", dockerfile)
        self.assertIn("FROM scratch", dockerfile)

    def test_mount_privilege_is_only_added_for_nonroot_and_original_paths_are_preserved(self):
        for uid in (0, 1001):
            with self.subTest(uid=uid), tempfile.TemporaryDirectory(prefix="original path ") as directory:
                scratch = Path(directory)
                disk = scratch / "original disk.qcow2"; builder = scratch / "original builder"; plan = scratch / "original plan.json"
                outputs = [json.dumps({"format": "qcow2", "format-specific": {"data": {}}}), '{"kind":"observed-root"}']
                with patch.object(distribution.os, "geteuid", return_value=uid), \
                     patch.object(distribution.subprocess, "check_output", side_effect=outputs) as read, \
                     patch.object(distribution.subprocess, "run") as execute:
                    self.assertEqual(distribution.readback_guest_root(disk, scratch, builder, plan, "a" * 64, 17, "b" * 64), {"kind": "observed-root"})
                command = read.call_args_list[-1].args[0]
                prefix = [] if uid == 0 else ["sudo", "-n"]
                self.assertEqual(command[:len(prefix)+1], prefix + ["unshare"])
                index = command.index("readback-root")
                self.assertEqual(command[index+1:index+4], [scratch / "root.raw", scratch / "root", builder])
                self.assertIn(f"--normal-build-plan={plan}", command)
                self.assertIn("loop,ro,noload,nodev,nosuid,noexec", command[index-1])
                for call in execute.call_args_list:
                    self.assertEqual(call.args[0][0], "qemu-img")


if __name__ == "__main__":
    unittest.main()
