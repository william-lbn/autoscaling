#!/bin/bash
set -Eeuo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/native-birth-source-stage-20261031T011639Z/run.py
