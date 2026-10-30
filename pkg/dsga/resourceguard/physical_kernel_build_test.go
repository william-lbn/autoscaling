package resourceguard

import "testing"

func TestPhysicalKernelBuildLookupKeepsExactReference(t *testing.T) {
	const profile = "pg17-linux-6.12.26-physical-component-v1"
	got, err := LookupPhysicalKernelBuild(profile)
	if err != nil || got.ProfileID != profile || got.KernelRelease != "6.12.26-william-bd33f7c78026" ||
		got.KernelArtifactSHA256 != "0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0" ||
		got.KernelConfigSHA256 != "639ea751eb4ae7743e2695368286d1aeb0417f6680d6ac24ae3b051948a94b96" ||
		got.KernelNotesSHA256 != "b52a6321c44be2b8d138e9ca990ec86413c3744a189a118ed69aa295cb8ef03d" {
		t.Fatalf("exact historical build reference differs: %+v %v", got, err)
	}
	// 调用者改值副本不能修改下一次 lookup 的编译来源。
	got.KernelNotesSHA256 = "changed"
	again, err := LookupPhysicalKernelBuild(profile)
	if err != nil || again.KernelNotesSHA256 == "changed" {
		t.Fatal("caller changed fixed build source")
	}
}

func TestPhysicalKernelBuildLookupRejectsAliases(t *testing.T) {
	for _, profile := range []string{"", "pg17-linux-6.12.26", "6.12.26", "pg17-linux-6.12.26-physical-component-v1 ", "pg17-linux-6.12.26-normal-v1-copy"} {
		t.Run(profile, func(t *testing.T) {
			got, err := LookupPhysicalKernelBuild(profile)
			if err == nil || got != (PhysicalKernelBuildReference{}) {
				t.Fatalf("unbuilt or nonexact build accepted: %+v %v", got, err)
			}
		})
	}
}
