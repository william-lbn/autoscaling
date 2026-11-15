package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

type normalImageReadback struct {
	Kind                string `json:"kind"`
	ProtocolVersion     uint32 `json:"protocol_version"`
	Architecture        string `json:"architecture"`
	BuildPlanSHA256     string `json:"build_plan_sha256"`
	BaseImageSHA256     string `json:"base_image_sha256"`
	SystemProfile       string `json:"system_profile"`
	SystemSHA256        string `json:"system_manifest_sha256"`
	NativeBirthSHA256   string `json:"native_birth_manifest_sha256"`
	PostgreSQLMajor     uint32 `json:"postgres_major"`
	PostgreSQLELFSHA256 string `json:"postgres_elf_sha256"`
}

// Readback uses the same closure builders as image generation against an
// independently supplied plan, base image digest and PostgreSQL ELF digest.
func readbackNormalImage(root string, planRaw []byte, baseSHA string, major uint32, postgresSHA string) (normalImageReadback, error) {
	var receipt normalImageReadback
	if !normalNativePGDigest(baseSHA) || !normalNativePGDigest(postgresSHA) {
		return receipt, fmt.Errorf("normal readback requires canonical base and PostgreSQL ELF digests")
	}
	plan, err := decodeNormalBuildPlan(planRaw)
	if err != nil {
		return receipt, err
	}
	image, err := newNormalNativeImage(root)
	if err != nil {
		return receipt, err
	}
	manifests := []struct {
		path     string
		limit    int64
		data     []byte
		identity os.FileInfo
	}{
		{path: normalPlanPath, limit: 64 << 10},
		{path: resourceguard.NormalSystemSourceManifestPath, limit: 1 << 20},
		{path: normalNativeBuildPath, limit: 32 << 10},
	}
	for i := range manifests {
		m := &manifests[i]
		m.data, m.identity, err = image.readManifest(m.path, m.limit)
		if err != nil {
			return receipt, err
		}
	}
	if !bytes.Equal(manifests[0].data, planRaw) {
		return receipt, fmt.Errorf("normal Guest plan differs from original build input")
	}

	systemInput, err := generateNormalSystemManifestForVersion(root, plan, baseSHA, 3)
	if err != nil {
		return receipt, err
	}
	system, err := resourceguard.BuildNormalSystemClosureManifestJSON(systemInput, image.canonical, image.open, image.lstat)
	if err != nil {
		return receipt, err
	}
	system = append(system, '\n')
	if !bytes.Equal(system, manifests[1].data) {
		return receipt, fmt.Errorf("normal Guest System3 closure differs from original disk manifest")
	}

	native, err := resourceguard.BuildNativeBirthManifestJSON(manifests[2].data, system, image.canonical, image.open, image.lstat)
	if err != nil {
		return receipt, err
	}
	if err := validateNormalNativePGManifest(native, major, postgresSHA); err != nil {
		return receipt, err
	}
	if !bytes.Equal(native, manifests[2].data) {
		return receipt, fmt.Errorf("normal Guest NativeBirth closure differs from original disk manifest")
	}
	for _, m := range manifests {
		raw, identity, err := image.readManifest(m.path, m.limit)
		if err != nil {
			return receipt, err
		}
		if !resourceguard.NativeBirthBuildFileUnchanged(m.identity, identity) || !bytes.Equal(m.data, raw) {
			return receipt, fmt.Errorf("normal Guest manifest changed before readback: %s", m.path)
		}
	}

	return normalImageReadback{
		Kind: "normal-guest-root-readback", ProtocolVersion: 1, Architecture: targetArchLinuxAmd64,
		BuildPlanSHA256: normalDigest(planRaw), BaseImageSHA256: baseSHA,
		SystemProfile: resourceguard.NormalSystemSourceV3ProfileID, SystemSHA256: normalDigest(system),
		NativeBirthSHA256: normalDigest(native), PostgreSQLMajor: major, PostgreSQLELFSHA256: postgresSHA,
	}, nil
}
