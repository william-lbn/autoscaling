#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-source-attempt06-20261101T004742Z-104172/worker.py >/data/neon-build/validation/normal-kernel-control-source-attempt06-20261101T004742Z-104172/unit.log 2>&1
