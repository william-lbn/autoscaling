#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/initial-boot-frame-20261031T164835Z-fa1fc6/worker.py /data/neon-build/validation/initial-boot-frame-20261031T164835Z-fa1fc6 20261031T164835Z-fa1fc6
