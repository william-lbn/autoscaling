#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-metadata-source-readback-20261101T021057Z-83399f/worker.py >/data/neon-build/validation/normal-kernel-metadata-source-readback-20261101T021057Z-83399f/unit.log 2>&1
