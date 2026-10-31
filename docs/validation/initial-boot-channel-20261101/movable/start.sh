#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/initial-boot-movable-20261031T170052Z-84be26/worker.py /data/neon-build/validation/initial-boot-movable-20261031T170052Z-84be26 20261031T170052Z-84be26
