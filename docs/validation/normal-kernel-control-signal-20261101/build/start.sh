#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-signal-go-20261101T015800Z-fd25ad/worker.py >/data/neon-build/validation/normal-kernel-control-signal-go-20261101T015800Z-fd25ad/unit.log 2>&1
