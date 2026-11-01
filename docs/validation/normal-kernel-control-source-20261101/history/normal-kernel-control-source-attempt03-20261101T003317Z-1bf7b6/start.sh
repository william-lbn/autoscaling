#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-source-attempt03-20261101T003317Z-1bf7b6/worker.py >/data/neon-build/validation/normal-kernel-control-source-attempt03-20261101T003317Z-1bf7b6/unit.log 2>&1
