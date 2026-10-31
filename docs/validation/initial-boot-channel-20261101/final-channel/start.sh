#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/initial-boot-channel-20261031T171244Z-5781c4/worker.py /data/neon-build/validation/initial-boot-channel-20261031T171244Z-5781c4 20261031T171244Z-5781c4
