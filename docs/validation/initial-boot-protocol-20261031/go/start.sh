#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/initial-boot-protocol-20261031T163800Z-4a1e3b/worker.py /data/neon-build/validation/initial-boot-protocol-20261031T163800Z-4a1e3b 20261031T163800Z-4a1e3b
