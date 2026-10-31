#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/fixed-runtime-iso-20261031T194311Z-0cabfe/worker.py /data/neon-build/validation/fixed-runtime-iso-20261031T194311Z-0cabfe 20261031T194311Z-0cabfe
