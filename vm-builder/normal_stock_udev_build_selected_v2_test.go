//go:build linux

package main

import (
	"archive/tar"
	"bytes"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

// 只检构建选择的实际声明字节，不以最小ELF fixture宣称可执行Source。
func TestNormalOrdinarySelectedBuildEmitsBroker3Controls2(t *testing.T) {
	root := normalV3BuilderFixture(t)
	plan := append(normalTestJSON(normalTestPlan()), '\n')
	guard := normalV3MustRead(t, filepath.Join(root, "/neonvm/bin/dsga-resource-guard"))
	if e := prepareNormalSystemV3AssetsForControls(root, plan, 2); e != nil {
		t.Fatal(e)
	}
	want, e := resourceguard.NormalSystemRoleBrokerDeclarationV3JSON(normalV3SHA(guard), normalV3SHA(plan))
	if e != nil {
		t.Fatal(e)
	}
	if got := normalV3MustRead(t, filepath.Join(root, resourceguard.NormalSystemRoleBrokerDeclarationPath)); !bytes.Equal(got, want) {
		t.Fatal("explicit2 lost broker3/five-FD declaration")
	}
	want, e = resourceguard.NormalSystemOrdinaryV2ManifestJSON()
	if e != nil {
		t.Fatal(e)
	}
	if got := normalV3MustRead(t, filepath.Join(root, resourceguard.NormalKernelControlSourceManifestPath)); !bytes.Equal(got, want) {
		t.Fatal("explicit2 retained old Root ordinary controls")
	}
	if e = prepareNormalSystemV3AssetsForControls(root, plan, 2); e == nil {
		t.Fatal("preexisting declarations adopted")
	}
}
func TestNormalOrdinarySelectedBuildTemplateCarriesLock(t *testing.T) {
	c := TemplatesContext{NormalSplit: true, NormalSystemMode: normalSplitV3BuildMode, NormalOrdinaryControlsVersion: 2}
	var buffer bytes.Buffer
	tw := tar.NewWriter(&buffer)
	if e := AddTemplatedFileToTar(tw, c, "Dockerfile", dockerfileVmBuilder); e != nil {
		t.Fatal(e)
	}
	if e := tw.Close(); e != nil {
		t.Fatal(e)
	}
	tr := tar.NewReader(&buffer)
	if _, e := tr.Next(); e != nil {
		t.Fatal(e)
	}
	raw, e := io.ReadAll(tr)
	if e != nil {
		t.Fatal(e)
	}
	for _, literal := range []string{"COPY normal-ordinary-package-lock.json /normal-ordinary-package-lock.json", "--normal-ordinary-controls-version=2 --normal-ordinary-package-lock=/normal-ordinary-package-lock.json", "--system-mode=normal-split-v3"} {
		if !strings.Contains(string(raw), literal) {
			t.Fatal("explicit selector or original lock lost", literal)
		}
	}
}
