#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 /data/neon-build/validation/initial-guest-channel-20261031T175137Z-0b2c55/worker.py /data/neon-build/validation/initial-guest-channel-20261031T175137Z-0b2c55 20261031T175137Z-0b2c55
