#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-metadata-native-20261101T024308Z-304d4f/worker.py >/data/neon-build/validation/normal-kernel-metadata-native-20261101T024308Z-304d4f/unit.log 2>&1
