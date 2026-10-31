set -eu
cp -a /input/system-fixture/. /fixture/
rm /fixture/neonvm/config/dsga-normal-system-source.json
/input/vm-builder --normal-manifest-root=/fixture --normal-build-plan=/proof/model-build-plan.json --normal-base-image-sha=abababababababababababababababababababababababababababababababab
cp /fixture/neonvm/config/dsga-normal-system-source.json /proof/generated-manifest.json
set +e
/input/vm-builder --normal-manifest-root=/fixture --normal-build-plan=/proof/model-build-plan.json --normal-base-image-sha=abababababababababababababababababababababababababababababababab >/proof/repeat.stdout 2>/proof/repeat.stderr
rc=$?
set -e
printf '%s\n' "$rc" >/proof/repeat.exitcode
[ "$rc" != 0 ]
