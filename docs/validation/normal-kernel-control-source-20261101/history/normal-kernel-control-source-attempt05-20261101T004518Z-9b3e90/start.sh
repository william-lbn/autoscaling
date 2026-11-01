#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-source-attempt05-20261101T004518Z-9b3e90/worker.py >/data/neon-build/validation/normal-kernel-control-source-attempt05-20261101T004518Z-9b3e90/unit.log 2>&1
