#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-memory-origin-v2-20261031T225813Z-ec5ace/native-worker.py >/data/neon-build/validation/normal-memory-origin-v2-20261031T225813Z-ec5ace/native-unit.log 2>&1
