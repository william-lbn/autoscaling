#!/bin/bash
set -uo pipefail
source /data/neon-build/config/env.sh
/usr/bin/docker buildx build --builder neon-data --progress plain --network none --build-arg TEST_NONCE=20261031T020138Z-attempt04 --output type=local,dest=/data/neon-build/validation/runtime-policy-templates-20261031T020138Z/attempt04 -f /data/neon-build/dev/runtime-policy-templates-20261031T020138Z/Dockerfile-attempt04 /data/neon-build/dev/runtime-policy-templates-20261031T020138Z > /data/neon-build/validation/runtime-policy-templates-20261031T020138Z/build-attempt04.log 2>&1
rc=$?
printf '%s\n' "$rc" > /data/neon-build/validation/runtime-policy-templates-20261031T020138Z/build-attempt04.rc
exit "$rc"
