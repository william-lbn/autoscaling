#!/bin/bash
set -uo pipefail
source /data/neon-build/config/env.sh
/usr/bin/docker buildx build --builder neon-data --progress plain --network none --build-arg TEST_NONCE=20261031T020138Z-before-entry-fix --output type=local,dest=/data/neon-build/validation/runtime-policy-templates-20261031T020138Z/before-entry-fix -f /data/neon-build/dev/runtime-policy-templates-20261031T020138Z/Dockerfile-before-entry-fix /data/neon-build/dev/runtime-policy-templates-20261031T020138Z > /data/neon-build/validation/runtime-policy-templates-20261031T020138Z/build-before-entry-fix.log 2>&1
rc=$?
printf '%s\n' "$rc" > /data/neon-build/validation/runtime-policy-templates-20261031T020138Z/build-before-entry-fix.rc
exit "$rc"
