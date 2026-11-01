#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-signal-go-20261101T015446Z-6712c8/worker.py >/data/neon-build/validation/normal-kernel-control-signal-go-20261101T015446Z-6712c8/unit.log 2>&1
