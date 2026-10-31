#!/bin/bash
set -uo pipefail
source /data/neon-build/config/env.sh
docker buildx build --builder neon-data --network host --allow network.host --progress plain --build-arg HTTP_PROXY --build-arg HTTPS_PROXY --build-arg NO_PROXY --build-arg http_proxy --build-arg https_proxy --build-arg no_proxy --build-arg ACTUAL_NONCE=bootstrap-presystem-bridge-20261031T004139Z --metadata-file /data/neon-build/validation/bootstrap-presystem-bridge-20261031T004139Z/build-metadata.json --output type=local,dest=/data/neon-build/validation/bootstrap-presystem-bridge-20261031T004139Z/export -f /data/neon-build/validation/bootstrap-presystem-bridge-20261031T004139Z/Dockerfile /data/neon-build/validation/bootstrap-presystem-bridge-20261031T004139Z > /data/neon-build/validation/bootstrap-presystem-bridge-20261031T004139Z/run.log 2>&1
rc=$?
printf '%s\n' "$rc" > /data/neon-build/validation/bootstrap-presystem-bridge-20261031T004139Z/exit-code.txt
exit "$rc"
