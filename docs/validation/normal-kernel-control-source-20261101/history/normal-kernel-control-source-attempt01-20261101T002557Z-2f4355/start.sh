#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-source-attempt01-20261101T002557Z-2f4355/worker.py >/data/neon-build/validation/normal-kernel-control-source-attempt01-20261101T002557Z-2f4355/unit.log 2>&1
