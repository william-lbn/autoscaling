import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import distribution


DISK = b"guest-disk"
KERNEL_ORIGINAL = b"earlier-base-kernel"
KERNEL_FINAL = b"native-kernel"


def checksum(raw):
    return hashlib.sha256(raw).hexdigest()


def append(archive, name, data):
    header = tarfile.TarInfo(name)
    header.size = len(data)
    archive.addfile(header, io.BytesIO(data))


class GuestOCI(unittest.TestCase):
    def build(self, archive, layers, architecture="amd64", image_index=False, *, compressed=True, diff_ids_override=None):
        def blob(data, media_type=None):
            key = checksum(data)
            append(archive, "blobs/sha256/" + key, data)
            result = {"digest": "sha256:" + key, "size": len(data)}
            if media_type:
                result["mediaType"] = media_type
            return result

        def json_blob(value):
            return blob(json.dumps(value).encode())

        collected = []
        diff_ids = []
        for entries in layers:
            content = io.BytesIO()
            with tarfile.open(mode="w:", fileobj=content) as layer:
                for name, data in entries:
                    append(layer, name, data)
            raw = content.getvalue()
            diff_ids.append("sha256:" + checksum(raw))
            if compressed:
                collected.append(blob(gzip.compress(raw, mtime=0), "application/vnd.oci.image.layer.v1.tar+gzip"))
            else:
                collected.append(blob(raw, "application/vnd.oci.image.layer.v1.tar"))
        if diff_ids_override is not None:
            diff_ids = diff_ids_override(diff_ids)
        config = json_blob({"os": "linux", "architecture": architecture, "rootfs": {"type": "layers", "diff_ids": diff_ids}})
        manifest = json_blob({"config": config, "layers": collected})
        if image_index:
            manifest["platform"] = {"os": "linux", "architecture": architecture}
            image = json_blob({"manifests": [manifest]})
        else:
            image = manifest
        append(archive, "index.json", json.dumps({"manifests": [image]}).encode())
        return image["digest"], manifest["digest"]

    def inspect(self, updates, kernel=KERNEL_FINAL, arch="amd64", image_index=False, **build_options):
        layers = [[("disk.qcow2", DISK), ("vmlinuz", KERNEL_ORIGINAL)], updates]
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "guest.oci.tar"
            with tarfile.open(path, "w:") as archive:
                image, platform = self.build(archive, layers, image_index=image_index, **build_options)
            value = distribution.inspect_guest_oci(path, image, arch, checksum(DISK), checksum(kernel))
        self.assertEqual(value["image_digest"], image)
        self.assertEqual(value["platform_manifest_digest"], platform)
        return value

    def test_final_overlay_selects_kernel_even_when_base_is_readable(self):
        result = self.inspect([("./vmlinuz", KERNEL_FINAL)], image_index=True)
        self.assertEqual([entry["material"]["sha256"] for entry in result["assets"]["vmlinuz"]],
                         [checksum(KERNEL_ORIGINAL), checksum(KERNEL_FINAL)])
        with self.assertRaisesRegex(ValueError, "Final Guest artifact.*vmlinuz"):
            self.inspect([("vmlinuz", KERNEL_FINAL)], kernel=KERNEL_ORIGINAL)

    def test_whiteout_hides_only_the_lower_layer(self):
        result = self.inspect([("disk.qcow2", DISK), ("vmlinuz", KERNEL_FINAL), (".wh..wh..opq", b"")])
        self.assertEqual(result["assets"]["disk.qcow2"][-1]["material"]["sha256"], checksum(DISK))
        with self.assertRaisesRegex(ValueError, "Final Guest artifact.*disk.qcow2"):
            self.inspect([("vmlinuz", KERNEL_FINAL), (".wh.disk.qcow2", b"")])

    def test_conflicting_paths_and_architecture_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Ambiguous or non-regular"):
            self.inspect([("vmlinuz", KERNEL_FINAL), ("./vmlinuz", KERNEL_ORIGINAL)])
        with self.assertRaisesRegex(ValueError, "ambiguous path"):
            self.inspect([("bin/../vmlinuz", KERNEL_FINAL)])
        with self.assertRaisesRegex(ValueError, "another operating system or architecture"):
            self.inspect([("vmlinuz", KERNEL_FINAL)], arch="arm64")
        with self.assertRaisesRegex(ValueError, "treats an artifact as a directory"):
            self.inspect([("vmlinuz/a", KERNEL_FINAL)])
        with self.assertRaisesRegex(ValueError, "Ambiguous or non-regular"):
            self.inspect([("vmlinuz", b"")])

    def test_rootfs_diff_ids_match_the_uncompressed_layer_order(self):
        updates = [("vmlinuz", KERNEL_FINAL)]
        compressed = self.inspect(updates)
        uncompressed = self.inspect(updates, compressed=False)
        self.assertEqual(compressed["rootfs_diff_ids"], uncompressed["rootfs_diff_ids"])
        with self.assertRaisesRegex(ValueError, "uncompressed layer differs"):
            self.inspect(updates, diff_ids_override=lambda ids: ids[::-1])
        with self.assertRaisesRegex(ValueError, "does not identify the uncompressed layers"):
            self.inspect(updates, diff_ids_override=lambda ids: ids[:1])

    def test_normal_readback_uses_final_disk_and_independent_inputs(self):
        expected = checksum(b"independent")
        receipt = {"kind": "normal-guest-root-readback", "protocol_version": 1, "architecture": "linux/amd64",
                   "build_plan_sha256": expected, "base_image_sha256": expected,
                   "system_profile": "neon-dsga-normal-system-source-v3", "postgres_major": 18,
                   "postgres_elf_sha256": expected, "system_manifest_sha256": expected,
                   "native_birth_manifest_sha256": expected}

        def read_disk(disk, scratch, builder, plan, base_sha, major, postgres_sha):
            self.assertEqual(disk.read_bytes(), DISK)
            self.assertEqual((base_sha, major, postgres_sha), (expected, 18, expected))
            scratch_paths.append(scratch)
            return receipt

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "guest.tar"
            with tarfile.open(path, "w:") as archive:
                image, _ = self.build(archive, [[("disk.qcow2", b"lower-disk"), ("vmlinuz", KERNEL_ORIGINAL)],
                                                [("disk.qcow2", DISK), ("vmlinuz", KERNEL_FINAL)]])
            builder = Path(directory) / "builder"
            plan = Path(directory) / "plan"
            for item in (builder, plan):
                item.write_bytes(b"independent")
            args = [path, image, checksum(DISK), checksum(KERNEL_FINAL), builder, expected,
                    plan, expected, expected, 18, expected]
            scratch_paths = []
            with patch.object(distribution, "readback_guest_root", side_effect=read_disk) as readback:
                result = distribution.verify_normal_guest_oci(*args)
                self.assertEqual(result["root"], receipt)
                self.assertEqual(result["oci"]["image_digest"], image)
                self.assertFalse(scratch_paths[0].exists())
                receipt["postgres_major"] = 17
                with self.assertRaisesRegex(ValueError, "root receipt differs"):
                    distribution.verify_normal_guest_oci(*args)
                args[3] = checksum(KERNEL_ORIGINAL)
                with self.assertRaisesRegex(ValueError, "Final Guest artifact.*vmlinuz"):
                    distribution.verify_normal_guest_oci(*args)
                self.assertEqual(readback.call_count, 2)
                plan.write_bytes(b"changed")
                with self.assertRaisesRegex(ValueError, "Builder or plan differs"):
                    distribution.verify_normal_guest_oci(*args)

    def test_qcow2_external_data_is_rejected_before_conversion(self):
        info = {"format": "qcow2", "format-specific": {"data": {"data-file": "outside.raw"}}}
        with patch.object(distribution.subprocess, "check_output", return_value=json.dumps(info).encode()):
            with patch.object(distribution.subprocess, "run") as run:
                with self.assertRaisesRegex(ValueError, "self-contained"):
                    distribution.readback_guest_root(Path("disk"), Path("scratch"), Path("builder"),
                                                     Path("plan"), checksum(DISK), 17, checksum(DISK))
                run.assert_not_called()
