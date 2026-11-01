#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-source-attempt02-20261101T002656Z-cb3118/worker.py >/data/neon-build/validation/normal-kernel-control-source-attempt02-20261101T002656Z-cb3118/unit.log 2>&1
