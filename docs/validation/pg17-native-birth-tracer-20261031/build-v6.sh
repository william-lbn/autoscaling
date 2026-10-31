#!/usr/bin/env bash
set -Eeuo pipefail
source /data/neon-build/config/env.sh
REPORT=/data/neon-build/validation/pg17-go-native-tracer-20261030T235629Z
set +e
docker buildx build --builder neon-data --network none --progress plain --build-arg NONCE=20261030T235629Z-v6 --metadata-file "$REPORT/build-v6-metadata.json" --output type=local,dest="$REPORT/artifact-v6" -f "$REPORT/Dockerfile-v2" /data/neon-build/dev/pg17-go-native-tracer-20261030T235629Z/cohort-410bcc 2>&1 | tee "$REPORT/build-v6.log"
rc=${PIPESTATUS[0]}
set -e
printf '%s\n' "$rc" > "$REPORT/build-v6-exit-code.txt"
exit "$rc"
