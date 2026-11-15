import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

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
    def build(self, archive, layers, architecture="amd64", image_index=False):
        def blob(data, media_type=None):
            key = checksum(data)
            append(archive, "blobs/sha256/" + key, data)
            result = {"digest": "sha256:" + key, "size": len(data)}
            if media_type:
                result["mediaType"] = media_type
            return result

        def json_blob(value):
            return blob(json.dumps(value).encode())

        config = json_blob({"os": "linux", "architecture": architecture})
        collected = []
        for entries in layers:
            content = io.BytesIO()
            with tarfile.open(mode="w:gz", fileobj=content) as layer:
                for name, data in entries:
                    append(layer, name, data)
            collected.append(blob(content.getvalue(), "application/vnd.oci.image.layer.v1.tar+gzip"))
        manifest = json_blob({"config": config, "layers": collected})
        if image_index:
            manifest["platform"] = {"os": "linux", "architecture": architecture}
            image = json_blob({"manifests": [manifest]})
        else:
            image = manifest
        append(archive, "index.json", json.dumps({"manifests": [image]}).encode())
        return image["digest"], manifest["digest"]

    def inspect(self, updates, kernel=KERNEL_FINAL, arch="amd64", image_index=False):
        layers = [[("disk.qcow2", DISK), ("vmlinuz", KERNEL_ORIGINAL)], updates]
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "guest.oci.tar"
            with tarfile.open(path, "w:") as archive:
                image, platform = self.build(archive, layers, image_index=image_index)
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
