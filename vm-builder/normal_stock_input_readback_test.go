package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalOrdinaryInputsRequireExactPackageAndSourceBytes(t *testing.T) {
	root := t.TempDir()
	apk := []byte("signed package input fixture")
	patch := []byte("original provider patch fixture")
	for name, data := range map[string][]byte{
		"apk/ordinary.apk": apk, "privsep-build/provider.patch": patch,
	} {
		file := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0700))
		require.NoError(t, os.WriteFile(file, data, 0600))
	}
	checksum := func(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }
	lock := normalStockAssetPackageLock{
		ProtocolVersion: 1,
		Scope:           normalStockAssetsLockScope,
		Inputs: map[string]string{
			"apk/ordinary.apk": checksum(apk), "privsep-build/provider.patch": checksum(patch),
		},
		Files: []normalStockAssetFileLock{
			{Path: "/usr/bin/ordinary", Bytes: 1, SHA256: checksum([]byte("x")), Input: "apk/ordinary.apk", Member: "usr/bin/ordinary"},
		},
	}
	raw, err := json.Marshal(lock)
	require.NoError(t, err)
	receipt, err := verifyNormalOrdinaryInputs(root, raw, checksum(raw))
	require.NoError(t, err)
	require.Equal(t, checksum(raw), receipt.PackageLockSHA256)
	require.Equal(t, 1, receipt.DerivedFileCount)
	require.Equal(t, []normalOrdinaryInput{
		{Path: "apk/ordinary.apk", SHA256: checksum(apk), Bytes: int64(len(apk))},
		{Path: "privsep-build/provider.patch", SHA256: checksum(patch), Bytes: int64(len(patch))},
	}, receipt.Inputs)

	_, err = verifyNormalOrdinaryInputs(root, raw, checksum(patch))
	require.ErrorContains(t, err, "package lock digest")

	provider := filepath.Join(root, "privsep-build", "provider.patch")
	require.NoError(t, os.WriteFile(provider, apk, 0600))
	_, err = verifyNormalOrdinaryInputs(root, raw, checksum(raw))
	require.ErrorContains(t, err, "byte count or digest differs")

	require.NoError(t, os.Remove(provider))
	require.NoError(t, os.WriteFile(filepath.Join(root, "same-patch"), patch, 0600))
	require.NoError(t, os.Symlink("../same-patch", provider))
	_, err = verifyNormalOrdinaryInputs(root, raw, checksum(raw))
	require.ErrorContains(t, err, "must be an original regular file")

	require.NoError(t, os.Rename(filepath.Join(root, "privsep-build"), filepath.Join(root, "source")))
	require.NoError(t, os.Symlink("source", filepath.Join(root, "privsep-build")))
	_, err = verifyNormalOrdinaryInputs(root, raw, checksum(raw))
	require.ErrorContains(t, err, "must be an original directory")

	lock.Inputs["apk/../outside"] = checksum(apk)
	raw, err = json.Marshal(lock)
	require.NoError(t, err)
	_, err = verifyNormalOrdinaryInputs(root, raw, checksum(raw))
	require.ErrorContains(t, err, "invalid staged ordinary input path")
}
