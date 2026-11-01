#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-prep-userns-v2-go-20261101T044252Z-f3ef01/worker.py >/data/neon-build/validation/normal-kernel-prep-userns-v2-go-20261101T044252Z-f3ef01/unit.log 2>&1
