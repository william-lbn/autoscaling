FROM 127.0.0.1:5001/neon/pg17-dev-tools@sha256:c940750ed64e18dbe1d5acda5f12b61bb3f1930be149140c54b4e09e62379ba1 AS test
WORKDIR /workspace
ENV GOPATH=/go GOMODCACHE=/go/pkg/mod GOCACHE=/root/.cache/go-build GOMAXPROCS=2 GOMEMLIMIT=512MiB
COPY . .
ARG NONCE
RUN --mount=type=cache,id=autoscaling-gomod-amd64-shared-v2,target=/go/pkg/mod,sharing=shared --mount=type=cache,id=autoscaling-gobuild-amd64,target=/root/.cache/go-build,sharing=shared set -eu; mkdir -p /out; sha256sum -c actual-source-sha256.txt >/out/actual-copy-verification.txt; echo 0 >/out/source.exitcode; cp actual-source-sha256.txt /out/compiled-source-sha256.txt; set +e; go test -race -count=1 -timeout=120s -json -run '^Test(ArrayRegistryWire|ArrayAuthority|ArrayCredential|ArrayHelperOriginal|NativeBirthSource|NativeBirthStage|NativeBirthTrace|ResourceHTTP|ConcurrentCallbacksShareMintAndRefreshProjectedFile|GuestCertificateRejectedBeforeProjectedBearerSent|GrantRejectsWrongBindingSchemaExpiryAndNoCache|ComputeIdentityChangePermanentlyClosesThisClient|OwnerCancellationStopsCacheAndInFlightMint|AuthorityAndWaiterDeadlinesAreIndependent|ChangedAuthorityNeverReturnsCachedToken)' ./pkg/dsga/resourceguard ./pkg/dsga/credential ./pkg/dsga/pgexec >/out/test.json 2>/out/test.stderr; echo $? >/out/test.exitcode
FROM scratch
COPY --from=test /out/ /
