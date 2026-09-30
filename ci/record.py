#!/usr/bin/env python3
import argparse
import json
import pathlib
import re
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--name", required=True)
parser.add_argument("--image", required=True)
parser.add_argument("--digest", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()
if not re.fullmatch(r"sha256:[a-f0-9]{64}", args.digest):
    raise ValueError("Missing image digest")
path = pathlib.Path(args.output)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps({"kind": "runtime", "name": args.name, "image": args.image,
    "digest": args.digest, "source_repository": "william-lbn/autoscaling",
    "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()}, indent=2) + "\n")
