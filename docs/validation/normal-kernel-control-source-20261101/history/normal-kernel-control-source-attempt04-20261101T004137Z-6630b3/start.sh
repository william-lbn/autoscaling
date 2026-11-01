#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-control-source-attempt04-20261101T004137Z-6630b3/worker.py >/data/neon-build/validation/normal-kernel-control-source-attempt04-20261101T004137Z-6630b3/unit.log 2>&1
