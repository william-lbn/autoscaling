#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-metadata-build-20261101T022218Z-a5db2e/worker.py
