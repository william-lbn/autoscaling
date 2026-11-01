#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-signal-native-20261101T015858Z-d00d6c/worker.py >/data/neon-build/validation/normal-kernel-control-signal-native-20261101T015858Z-d00d6c/unit.log 2>&1
