#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/initial-runner-production-20261031T190225Z-19132f/worker.py /data/neon-build/validation/initial-runner-production-20261031T190225Z-19132f 20261031T190225Z-19132f
