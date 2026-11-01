#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-prep-userns-v2-go-20261101T045408Z-63b78f/worker.py >/data/neon-build/validation/normal-kernel-prep-userns-v2-go-20261101T045408Z-63b78f/unit.log 2>&1
