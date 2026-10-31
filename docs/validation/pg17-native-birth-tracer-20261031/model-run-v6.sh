#!/bin/bash
set -Eeuo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/pg17-go-native-tracer-20261030T235629Z/model-run-v6.py
