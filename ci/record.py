#!/usr/bin/env python3
import argparse
import os
import subprocess

from distribution import LOCK, DIGEST, execution_identity, validate_record, validate_attempt, verify_source, write_json

parser = argparse.ArgumentParser()
parser.add_argument("--name", required=True)
parser.add_argument("--image", required=True)
parser.add_argument("--digest", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--arch", default=os.environ.get("ARCH"), required="ARCH" not in os.environ)
parser.add_argument("--source-commit", default=os.environ.get("RELEASE_SOURCE"), required="RELEASE_SOURCE" not in os.environ)
parser.add_argument("--dependency", action="append", default=[], help="Consumed image name=sha256:digest")
args = parser.parse_args()
verify_source(args.source_commit)
dependencies = {}
for item in args.dependency:
    name, separator, digest = item.partition("=")
    if not separator or name in dependencies or not DIGEST.fullmatch(digest):
        raise ValueError("Invalid or duplicate pinned dependency")
    dependencies[name] = digest
identity = execution_identity()
version = args.image.rsplit(":", 1)[-1]
validate_attempt(version, args.arch, identity)
record = {"kind": "runtime", "name": args.name, "image": args.image,
          "digest": args.digest, "source_repository": LOCK["source_repository"],
          "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
          "architecture": "linux/" + args.arch, "execution": identity, "dependencies": dependencies}
validate_record(record, args.source_commit, version, args.arch, identity)
write_json(args.output, record)
