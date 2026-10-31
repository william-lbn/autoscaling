#!/bin/bash
set -Eeuo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/native-birth-source-elf-fix-20261031T013733Z/run.py
