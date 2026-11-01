#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-metadata-patch-base-readback-20261101T023444Z-2f3d26/worker.py >/data/neon-build/validation/normal-kernel-metadata-patch-base-readback-20261101T023444Z-2f3d26/unit.log 2>&1
