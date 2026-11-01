#!/bin/bash
set -euo pipefail
source /data/neon-build/config/env.sh
exec python3 -u /data/neon-build/validation/normal-kernel-prep-userns-v2-native-20261101T050118Z-678594/run-kvm.py >/data/neon-build/validation/normal-kernel-prep-userns-v2-native-20261101T050118Z-678594/runner.log 2>&1
