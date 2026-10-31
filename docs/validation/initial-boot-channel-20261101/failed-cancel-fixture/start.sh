#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/initial-boot-channel-20261031T170807Z-423d74/worker.py /data/neon-build/validation/initial-boot-channel-20261031T170807Z-423d74 20261031T170807Z-423d74
