package main

import (
	"archive/tar"
	"bytes"
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/alessio/shellescape"
	"github.com/distribution/reference"
	cliconfig "github.com/docker/cli/cli/config"
	dockercontext "github.com/docker/cli/cli/context"
	contextstore "github.com/docker/cli/cli/context/store"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

// vm-builder --src alpine:3.19 --dst vm-alpine:dev --file vm-alpine.qcow2

var (
	//go:embed files/img.Dockerfile
	dockerfileVmBuilder string
	//go:embed files/helper.move-bins.sh
	scriptMoveBinsHelper string
	//go:embed files/vmstart
	scriptVmStart string
	//go:embed files/inittab
	scriptInitTab string
	//go:embed files/vmacpi
	scriptVmAcpi string
	//go:embed files/vmshutdown
	scriptVmShutdown string
	//go:embed files/vminit
	scriptVmInit string
	//go:embed files/dsga-resource-service
	scriptDSGAResourceService string
	//go:embed files/normal-split/vminit
	scriptNormalVmInit string
	//go:embed files/normal-split/dsga-resource-service
	scriptNormalResourceService string
	//go:embed files/normal-split/trusted-controller-resource-service
	scriptTrustedControllerResourceService string
	//go:embed files/normal-split/vmshutdown
	scriptNormalShutdown string
	//go:embed files/normal-split/udev-init.sh
	scriptNormalUdevInit string
	//go:embed files/normal-split/prepare-root.sh
	scriptNormalPrepareRoot string
	//go:embed files/udev-init.sh
	scriptUdevInit string
	//go:embed files/resize-swap.sh
	scriptResizeSwap string
	//go:embed files/set-disk-quota.sh
	scriptSetDiskQuota string
	//go:embed files/vector.yaml
	configVector string
	//go:embed files/chrony.conf
	configChrony string
	//go:embed files/sshd_config
	configSshd string
)

const (
	targetArchLinuxAmd64 = "linux/amd64"
	targetArchLinuxArm64 = "linux/arm64"
)

var (
	Version           string
	NeonvmDaemonImage string

	BusyboxImageTag      string
	BusyboxImageShaAmd64 string
	BusyboxImageShaArm64 string

	AlpineImageTag      string
	AlpineImageShaAmd64 string
	AlpineImageShaArm64 string

	systemModeFlag                  = flag.String("system-mode", "legacy", "Fixed System build selector: legacy | normal-split-v1 | normal-split-v3 (not runtime authorization)")
	normalPlanFlag                  = flag.String("normal-build-plan", "", "Explicit versioned fixed compute build plan; values are not logged")
	normalNativeBirthVersion        = flag.Uint("normal-native-birth-version", 0, "Explicit build-only NativeBirth collector: 0 old behavior | 1 fixed GNU image")
	normalNativePGMajor             = flag.Uint("normal-native-pg-major", 17, "Private image PG major selection: 17 default | 18 explicit; not collector ABI")
	normalNativePostgresSHA         = flag.String("normal-native-postgres-sha256", "", "Original PostgreSQL ELF SHA256 from trusted build input; required for explicit PG18")
	normalManifestRoot              = flag.String("normal-manifest-root", "", "Build-container-only final root inventory command")
	normalPrepareConfigsOnly        = flag.Bool("normal-prepare-configs-only", false, "Private pre-base preparation of canonical controls2 configurations only; emits no System/Native inventory")
	normalPrepareTrustedComputeOnly = flag.Bool("normal-prepare-trusted-compute-only", false, "Explicit pre-base trusted-controller-v1 launcher refresh from the fixed build plan; no inventory or runtime authorization")
	normalBaseDigest                = flag.String("normal-base-image-sha", "", "Original base image declaration for final inventory; not final image self hash")
	normalOrdinaryControls          = flag.Uint("normal-ordinary-controls-version", 1, "Explicit fixed ordinary controls version: 1 or 2; version 2 requires normal-split-v3 and original package lock")
	normalOrdinaryPackageLock       = flag.String("normal-ordinary-package-lock", "", "Original Linux source/package build lock for explicit ordinary controls version 2")
	srcImage                        = flag.String("src", "", `Docker image used as source for virtual machine disk image: --src=alpine:3.19`)
	dstImage                        = flag.String("dst", "", `Docker image with resulting disk image: --dst=vm-alpine:3.19`)
	size                            = flag.String("size", "1G", `Size for disk image: --size=1G`)
	outFile                         = flag.String("file", "", `Save disk image as file: --file=vm-alpine.qcow2`)
	buildArgs                       = MultiStringFlag("build-arg", "Docker build-args to use during the build (multiple allowed)")
	specFile                        = flag.String("spec", "", `File containing additional customization: --spec=spec.yaml`)
	quiet                           = flag.Bool("quiet", false, `Show less output from the docker build process`)
	forcePull                       = flag.Bool("pull", false, `Pull src image even if already present locally`)
	version                         = flag.Bool("version", false, `Print vm-builder version`)

	daemonImageFlag  = flag.String("daemon-image", "", `Specify the neonvm-daemon image: --daemon-image=neonvm-daemon:dev`)
	alpineImageFlag  = flag.String("alpine-image", "alpine", `Specify the alpine image: --alpine-image=docker.io/library/alpine`)
	busyboxImageFlag = flag.String("busybox-image", "busybox", `Specify the busybox image: --busybox-image=docker.io/library/busybox`)
	targetArch       = flag.String("target-arch", "", fmt.Sprintf("Target architecture: --arch %s | %s", targetArchLinuxAmd64, targetArchLinuxArm64))
)

func AddTemplatedFileToTar(tw *tar.Writer, tmplArgs any, filename string, tmplString string) error {
	tmpl, err := template.New(filename).Parse(tmplString)
	if err != nil {
		return fmt.Errorf("failed to parse template for %q: %w", filename, err)
	}

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, tmplArgs); err != nil {
		return fmt.Errorf("failed to execute template for %q: %w", filename, err)
	}
	return addFileToTar(tw, filename, buf.Bytes())
}

func addFileToTar(tw *tar.Writer, filename string, contents []byte) error {
	tarHeader := &tar.Header{
		Name: filename,
		Size: int64(len(contents)),
		Mode: 0o755, // TODO: shouldn't just set this for everything.
	}

	if err := tw.WriteHeader(tarHeader); err != nil {
		return fmt.Errorf("failed to write tar header for %q: %w", filename, err)
	}
	if _, err := tw.Write(contents); err != nil {
		return fmt.Errorf("failed to write file content for %q: %w", filename, err)
	}

	return nil
}

type TemplatesContext struct {
	User          string
	Entrypoint    []string
	Cmd           []string
	Env           []string
	RootDiskImage string

	NeonvmDaemonImage string

	BusyboxImage    string
	BusyboxImageTag string
	BusyboxImageSha string
	AlpineImage     string
	AlpineImageTag  string
	AlpineImageSha  string

	SpecBuild                     string
	SpecMerge                     string
	InittabCommands               []inittabCommand
	AgettyTTY                     string
	ShutdownHook                  string
	NormalSystemMode              string
	NormalOrdinaryControlsVersion uint32
	NormalNativeBirthVersion      uint32
	NormalNativePGMajor           uint32
	NormalNativePostgresSHA       string
	NormalSplit                   bool
	NormalBaseSHA                 string
	NormalUID                     uint32
	NormalGID                     uint32
}

type inittabCommand struct {
	SysvInitAction      string
	CommandUser         string
	ShellEscapedCommand string
}

func main() {
	flag.Parse()
	if *normalPrepareTrustedComputeOnly && (*normalManifestRoot == "" || *normalPrepareConfigsOnly) {
		log.Fatal("trusted Compute preparation requires an explicit root and a separate action")
	}
	if *normalPrepareConfigsOnly && *normalManifestRoot == "" {
		log.Fatal("configuration preparation requires explicit image root")
	}
	if e := validateNormalNativePGSelection(*systemModeFlag, *targetArch, *normalOrdinaryControls, *normalNativeBirthVersion, *normalNativePGMajor, *normalNativePostgresSHA); e != nil {
		log.Fatal(e)
	}
	// 仅构建容器的固定库存命令，发生在完整rootdisk和库alias之后；不连接Docker/网络。
	if *normalManifestRoot != "" {
		raw, e := os.ReadFile(*normalPlanFlag)
		if e != nil {
			log.Fatal("normal plan unavailable")
		}
		plan, e := decodeNormalBuildPlan(raw)
		if e != nil {
			log.Fatal(e)
		}
		if *normalPrepareTrustedComputeOnly {
			if *normalOrdinaryControls != 2 {
				log.Fatal("trusted Compute preparation requires exact ordinary controls version 2")
			}
			if e := normalPrepareTrustedComputeLauncher(*normalManifestRoot, *systemModeFlag, uint32(*normalOrdinaryControls), plan, *normalBaseDigest); e != nil {
				log.Fatal(e)
			}
			return
		}
		packageLock, e := readNormalOrdinaryV2BuildLock(*systemModeFlag, *normalOrdinaryControls, *normalOrdinaryPackageLock)
		if e != nil {
			log.Fatal(e)
		}
		// 同一正式Builder既可在base构建前仅物化配置，也在Collector写任何库存前
		// 严格核真实配置字节。prepare-only不写旧System/Native，不连接Docker/网络。
		if e := normalProductionConfigurationPhase(*normalManifestRoot, *systemModeFlag, uint32(*normalOrdinaryControls), *normalPrepareConfigsOnly, *normalBaseDigest); e != nil {
			log.Fatal(e)
		}
		if *normalPrepareConfigsOnly {
			return
		}
		version := uint32(2)
		if *systemModeFlag == normalSplitV3BuildMode {
			version = 3
			if *normalOrdinaryControls == 2 {
				if _, e := prepareNormalSystemStockUdevAssets(*normalManifestRoot, packageLock, plan.ComputeUID, plan.ComputeGID, 2); e != nil {
					log.Fatal(e)
				}
			}
			if e := prepareNormalSystemV3AssetsForControls(*normalManifestRoot, raw, uint32(*normalOrdinaryControls)); e != nil {
				log.Fatal(e)
			}
		} else if *systemModeFlag != "legacy" && *systemModeFlag != normalSplitBuildMode {
			log.Fatal("unsupported final inventory mode")
		}
		b, e := generateNormalSystemManifestForVersion(*normalManifestRoot, plan, *normalBaseDigest, version)
		if e != nil {
			log.Fatal(e)
		}
		if *normalNativeBirthVersion == 1 {
			if e := publishCompleteNormalNativeBirthImageForPG(*normalManifestRoot, b, uint32(*normalNativePGMajor), *normalNativePostgresSHA); e != nil {
				log.Fatal(e)
			}
			return
		}
		f, e := os.OpenFile(filepath.Join(*normalManifestRoot, resourceguard.NormalSystemSourceManifestPath), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
		if e != nil {
			log.Fatal(e)
		}
		if _, e = f.Write(append(b, '\n')); e != nil {
			log.Fatal(e)
		}
		if e = f.Close(); e != nil {
			log.Fatal(e)
		}
		return
	}
	var dstIm string
	if *version {
		fmt.Println(Version)
		os.Exit(0)
	}
	if len(*daemonImageFlag) == 0 && len(NeonvmDaemonImage) == 0 {
		log.Println("neonvm-daemon image not set, needs to be explicitly passed in, or compiled with -ldflags '-X main.NeonvmDaemonImage=...'")
		flag.PrintDefaults()
		os.Exit(1)
	}

	alpineImage := *alpineImageFlag
	busyboxImage := *busyboxImageFlag

	if targetArch == nil || *targetArch == "" {
		log.Println("Target architecture not set, see usage info:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *targetArch != targetArchLinuxAmd64 && *targetArch != targetArchLinuxArm64 {
		log.Fatalf("Unsupported target architecture: %q", *targetArch)
		flag.PrintDefaults()
		return
	}

	neonvmDaemonImage := NeonvmDaemonImage
	if len(*daemonImageFlag) != 0 {
		neonvmDaemonImage = *daemonImageFlag
	}

	if len(*srcImage) == 0 {
		log.Println("-src not set, see usage info:")
		flag.PrintDefaults()
		os.Exit(1)
	}
	if len(*dstImage) == 0 {
		dstIm = fmt.Sprintf("vm-%s", *srcImage)
		log.Printf("-dst not set, using %s\n", dstIm)
	} else {
		dstIm = *dstImage
	}

	var spec *imageSpec
	if *specFile != "" {
		var err error
		spec, err = readImageSpec(*specFile)
		if err != nil {
			log.Fatalln(err)
			os.Exit(1)
		}
	}

	var normalRaw []byte
	var normalPlan normalBuildPlan
	if *normalPlanFlag != "" {
		var e error
		normalRaw, e = os.ReadFile(*normalPlanFlag)
		if e != nil {
			log.Fatal("normal build plan unavailable")
		}
	}
	if e := validateNormalBuildSelection(*systemModeFlag, *targetArch, spec, *buildArgs, normalRaw); e != nil {
		log.Fatal(e)
	}
	if *systemModeFlag == normalSplitBuildMode || *systemModeFlag == normalSplitV3BuildMode {
		normalPlan, _ = decodeNormalBuildPlan(normalRaw)
	}
	ordinaryLockRaw, e := readNormalOrdinaryV2BuildLock(*systemModeFlag, *normalOrdinaryControls, *normalOrdinaryPackageLock)
	if e != nil {
		log.Fatal(e)
	}
	log.Println("Load docker credentials")
	dockerConfig, err := cliconfig.Load("" /* auto-detect right directory */)
	if err != nil {
		log.Fatalln(err)
	}

	credentials, err := dockerConfig.GetAllCredentials()
	if err != nil {
		log.Fatalln(err)
	}
	authConfigs := make(map[string]registry.AuthConfig)
	for key, value := range credentials {
		log.Printf("Found docker credentials for %s", key)
		authConfigs[key] = registry.AuthConfig(value)
	}

	dockerClientOpts := []client.Opt{
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	}

	if dockerConfig.CurrentContext != "" {
		store := contextstore.New(
			cliconfig.ContextStoreDir(),
			contextstore.NewConfig(
				contextstore.TypeGetter(func() any { return new(contextstore.Metadata) }),
				contextstore.EndpointTypeGetter("docker", contextstore.TypeGetter(func() any { return new(dockercontext.EndpointMetaBase) })),
			),
		)

		meta, err := store.GetMetadata(dockerConfig.CurrentContext)
		if err != nil {
			log.Fatalf("failed to get metadata for docker context %q: %s", dockerConfig.CurrentContext, err)
		} else if _, ok := meta.Endpoints["docker"]; !ok {
			log.Fatalf("metadata for docker context %q missing \"docker\" endpoint", dockerConfig.CurrentContext)
		}

		endpointMeta := meta.Endpoints["docker"].(dockercontext.EndpointMetaBase)
		if endpointMeta.Host == "" {
			log.Fatalf("docker endpoint host for context %q is missing", dockerConfig.CurrentContext)
		}

		dockerClientOpts = append(dockerClientOpts, client.WithHost(endpointMeta.Host))
	}

	ctx := context.Background()

	log.Println("Setup docker connection")
	cli, err := client.NewClientWithOpts(dockerClientOpts...)
	if err != nil {
		log.Fatalln(err)
	}
	defer cli.Close()

	hostContainsSrcImage := false
	if !*forcePull {
		hostImages, err := cli.ImageList(ctx, types.ImageListOptions{})
		if err != nil {
			log.Fatalln(err) //nolint:gocritic // linter complains that Fatalln circumvents deferred cli.Close(). Too much work to fix in #721, leaving for later.
		}

		for _, img := range hostImages {
			for _, name := range img.RepoTags {
				if name == *srcImage {
					hostContainsSrcImage = true
					break
				}
			}
			if hostContainsSrcImage {
				break
			}
		}
	}

	if !hostContainsSrcImage {
		// pull source image
		// use a closure so deferred close is closer
		err := func() error {
			named, err := reference.ParseNormalizedNamed(*srcImage)
			if err != nil {
				return err
			}
			reg := reference.Domain(named)

			imagePullOptions := types.ImagePullOptions{}
			if authConfig, ok := authConfigs[reg]; ok {
				encoded, err := registry.EncodeAuthConfig(authConfig)
				if err != nil {
					return err
				}
				imagePullOptions.RegistryAuth = encoded
			} else {
				// Special case handling of docker.io weirdness.
				// ref https://github.com/moby/moby/blob/e7347f8a8c2fd3d2abd34b638d6fc8c18b0278d1/registry/config.go#L26-L49
				// (and other handling around index.docker.io in that file...)
				//
				// See also e.g. https://github.com/containrrr/watchtower/issues/1176
				legacyConfig, hasLegacyDockerConfig := authConfigs["https://index.docker.io/v1/"]
				if hasLegacyDockerConfig && (reg == "docker.io" || reg == "registry-1.docker.io") {
					encoded, err := registry.EncodeAuthConfig(legacyConfig)
					if err != nil {
						return err
					}
					imagePullOptions.RegistryAuth = encoded
				} else {
					log.Printf("No docker credentials found for %s", reg)
				}
			}

			log.Printf("Pull source docker image: %s", *srcImage)
			pull, err := cli.ImagePull(ctx, *srcImage, imagePullOptions)
			if err != nil {
				return err
			}
			defer pull.Close()
			// do quiet pull - discard output
			_, err = io.Copy(io.Discard, pull)
			return err
		}()
		if err != nil {
			log.Fatalln(err)
		}

	}

	log.Printf("Build docker image for virtual machine (disk size %s): %s\n", *size, dstIm)
	imageSpec, _, err := cli.ImageInspectWithRaw(ctx, *srcImage)
	if err != nil {
		log.Fatalln(err)
	}

	// Shell-escape all the command pieces, twice. We need to do it twice because we're generating
	// a shell script that appends these to a second shell script.
	for i := range imageSpec.Config.Entrypoint {
		imageSpec.Config.Entrypoint[i] = shellescape.Quote(shellescape.Quote(imageSpec.Config.Entrypoint[i]))
	}
	for i := range imageSpec.Config.Cmd {
		imageSpec.Config.Cmd[i] = shellescape.Quote(shellescape.Quote(imageSpec.Config.Cmd[i]))
	}

	tmplArgs := TemplatesContext{
		User:          "root", // overridden below, if imageSpec.Config.User != ""
		Entrypoint:    imageSpec.Config.Entrypoint,
		Cmd:           imageSpec.Config.Cmd,
		Env:           imageSpec.Config.Env,
		RootDiskImage: *srcImage,

		NeonvmDaemonImage: neonvmDaemonImage,

		BusyboxImage:    busyboxImage,
		BusyboxImageTag: BusyboxImageTag,
		BusyboxImageSha: getImageSha(*targetArch, BusyboxImageShaAmd64, BusyboxImageShaArm64),

		AlpineImage:    alpineImage,
		AlpineImageTag: AlpineImageTag,
		AlpineImageSha: getImageSha(*targetArch, AlpineImageShaAmd64, AlpineImageShaArm64),

		SpecBuild:       "",  // overridden below if spec != nil
		SpecMerge:       "",  // overridden below if spec != nil
		InittabCommands: nil, // overridden below if spec != nil
		ShutdownHook:    "",  // overridden below if spec != nil
		AgettyTTY:       getAgettyTTY(*targetArch),
	}

	if len(imageSpec.Config.User) != 0 {
		tmplArgs.User = imageSpec.Config.User
	}

	if *systemModeFlag == normalSplitBuildMode || *systemModeFlag == normalSplitV3BuildMode {
		tmplArgs.NormalSplit = true
		tmplArgs.NormalSystemMode = *systemModeFlag
		tmplArgs.NormalOrdinaryControlsVersion = uint32(*normalOrdinaryControls)
		tmplArgs.NormalNativeBirthVersion = uint32(*normalNativeBirthVersion)
		tmplArgs.NormalNativePGMajor = uint32(*normalNativePGMajor)
		tmplArgs.NormalNativePostgresSHA = *normalNativePostgresSHA
		tmplArgs.NormalUID = normalPlan.ComputeUID
		tmplArgs.NormalGID = normalPlan.ComputeGID
		tmplArgs.User = fmt.Sprintf("%d:%d", normalPlan.ComputeUID, normalPlan.ComputeGID)
		tmplArgs.Entrypoint = nil
		tmplArgs.Cmd = nil
		tmplArgs.Env = nil
		tmplArgs.AgettyTTY = ""
		pinned, declared, e := normalPinnedBase(*srcImage, imageSpec.ID, imageSpec.RepoDigests)
		if e != nil {
			log.Fatal(e)
		}
		tmplArgs.RootDiskImage = pinned
		tmplArgs.NormalBaseSHA = declared
		for _, v := range []string{neonvmDaemonImage, busyboxImage + ":" + BusyboxImageTag + tmplArgs.BusyboxImageSha, alpineImage + ":" + AlpineImageTag + tmplArgs.AlpineImageSha} {
			if e := normalRequirePinnedImage(v); e != nil {
				log.Fatal(e)
			}
		}
	}
	tarBuffer := new(bytes.Buffer)
	tw := tar.NewWriter(tarBuffer)
	defer tw.Close()

	if spec != nil {
		tmplArgs.SpecBuild = spec.Build
		tmplArgs.SpecMerge = spec.Merge
		tmplArgs.ShutdownHook = strings.ReplaceAll(spec.ShutdownHook, "\n", "\n\t")

		for _, c := range spec.Commands {
			// Allow core dumps for all inittab targets
			c.Shell = fmt.Sprintf("ulimit -c unlimited; %s", c.Shell)
			tmplArgs.InittabCommands = append(tmplArgs.InittabCommands, inittabCommand{
				SysvInitAction:      c.SysvInitAction,
				CommandUser:         c.User,
				ShellEscapedCommand: shellescape.Quote(c.Shell),
			})
		}
		for _, f := range spec.Files {
			var contents []byte
			switch {
			case f.Content != nil:
				contents = []byte(*f.Content)
			case f.HostPath != nil:
				// the 'host path' is relative to the directory that the spec file is in
				path := filepath.Join(filepath.Dir(*specFile), *f.HostPath)

				var err error
				contents, err = os.ReadFile(path)
				if err != nil {
					err = fmt.Errorf("failed to read file %q: %w", path, err)
					log.Fatalln(err)
				}
			}

			if err := addFileToTar(tw, f.Filename, contents); err != nil {
				log.Fatalln(err)
			}
		}
	}

	files := []struct {
		filename string
		tmpl     string
	}{
		{"Dockerfile", dockerfileVmBuilder},
		{"helper.move-bins.sh", scriptMoveBinsHelper},
		{"vmstart", scriptVmStart},
		{"vmshutdown", scriptVmShutdown},
		{"inittab", scriptInitTab},
		{"vmacpi", scriptVmAcpi},
		{"vminit", scriptVmInit},
		{"dsga-resource-service", scriptDSGAResourceService},
		{"vector.yaml", configVector},
		{"chrony.conf", configChrony},
		{"sshd_config", configSshd},
		{"udev-init.sh", scriptUdevInit},
		{"resize-swap.sh", scriptResizeSwap},
		{"set-disk-quota.sh", scriptSetDiskQuota},
	}

	normalLiteralFiles := map[string]bool{}
	if tmplArgs.NormalSplit {
		version := uint32(2)
		if *systemModeFlag == normalSplitV3BuildMode {
			version = 3
		}
		entries, e := resourceguard.NormalSystemSourceInitEntriesForVersion(version)
		if e != nil {
			log.Fatal(e)
		}
		if normalPlan.ExecutionModel == resourceguard.TrustedControllerExecutionModel {
			entries = resourceguard.TrustedControllerInitEntries()
		}

		replacements := map[string]string{"inittab": strings.Join(entries, "\n") + "\n", "vminit": scriptNormalVmInit, "vmstart": normalComputeScript(normalPlan), "dsga-resource-service": scriptNormalResourceService, "vmshutdown": scriptNormalShutdown, "udev-init.sh": scriptNormalUdevInit}
		if version == 3 {
			replacements["vmstart"] = normalComputeScriptV3()
			// env-i在exec前构造真实proc/environ；os.Clearenv不能证明原始来源。
			replacements["vminit"] = strings.Replace(scriptNormalVmInit, "exec /neonvm/bin/dsga-resource-guard initial-boot", "exec /neonvm/bin/busybox env -i PATH=/neonvm/bin /neonvm/bin/dsga-resource-guard initial-boot", 1)
		}
		if normalPlan.ExecutionModel == resourceguard.TrustedControllerExecutionModel {
			replacements["vmstart"] = normalTrustedComputeScript(normalPlan)
			// 可信System服务只读复核当前Held；PG/Compute出生继续使用原空域门。
			replacements["dsga-resource-service"] = scriptTrustedControllerResourceService
		}
		for i := range files {
			if v, ok := replacements[files[i].filename]; ok {
				files[i].tmpl = v
				normalLiteralFiles[files[i].filename] = true
			}
		}
		if *normalOrdinaryControls == 2 {
			assets, e := normalOrdinaryV2BuildAssets(2)
			if e != nil {
				log.Fatal(e)
			}
			for _, a := range assets {
				name := map[string]string{"/neonvm/bin/vmstart": "vmstart", "/neonvm/bin/vmshutdown": "vmshutdown", "/neonvm/config/vector.yaml": "vector.yaml", "/neonvm/config/chrony.conf": "chrony.conf"}[a.Path]
				if name == "" {
					log.Fatal("unknown fixed ordinary asset")
				}
				for i := range files {
					if files[i].filename == name {
						files[i].tmpl = string(a.Bytes)
						normalLiteralFiles[name] = true
					}
				}
			}
			if e := addFileToTar(tw, "normal-ordinary-package-lock.json", ordinaryLockRaw); e != nil {
				log.Fatal(e)
			}
		}
		tool, e := normalBuilderExecutable()
		if e != nil {
			log.Fatal(e)
		}
		for _, f := range []struct {
			n string
			b []byte
		}{{"normal-manifest-tool", tool}, {"normal-build-plan.json", normalRaw}, {"normal-prepare-root.sh", []byte(scriptNormalPrepareRoot)}} {
			if e := addFileToTar(tw, f.n, f.b); e != nil {
				log.Fatal(e)
			}
		}
	}
	for _, f := range files {
		var e error
		if normalLiteralFiles[f.filename] {
			e = addFileToTar(tw, f.filename, []byte(f.tmpl))
		} else {
			e = AddTemplatedFileToTar(tw, tmplArgs, f.filename, f.tmpl)
		}
		if e != nil {
			log.Fatalln(e)
		}
	}

	finalBuildArgs := make(map[string]*string)
	finalBuildArgs["VM_BUILDER_DISK_SIZE"] = size
	finalBuildArgs["VM_BUILDER_TARGET_ARCH"] = targetArch
	for _, arg := range *buildArgs {
		kv := strings.SplitN(arg, "=", 2)
		if len(kv) != 2 {
			log.Fatalf("unexpected build arg %q, must have \"key=value\" syntax", kv)
		}

		finalBuildArgs[kv[0]] = &kv[1]
	}

	opt := types.ImageBuildOptions{
		AuthConfigs:    authConfigs,
		Tags:           []string{dstIm},
		BuildArgs:      finalBuildArgs,
		SuppressOutput: *quiet,
		NoCache:        false,
		Context:        tarBuffer,
		Dockerfile:     "Dockerfile",
		Remove:         true,
		ForceRemove:    true,
		Platform:       *targetArch,
	}
	buildResp, err := cli.ImageBuild(ctx, tarBuffer, opt)
	if err != nil {
		log.Fatalln(err)
	}

	defer buildResp.Body.Close()

	out := io.Writer(os.Stdout)
	if *quiet {
		out = io.Discard
	}
	err = jsonmessage.DisplayJSONMessagesStream(buildResp.Body, out, os.Stdout.Fd(), term.IsTerminal(int(os.Stdout.Fd())), nil)
	if err != nil {
		log.Fatalln(err)
	}

	if len(*outFile) != 0 {
		log.Printf("Save disk image as %s", *outFile)
		// create container from docker image we just built
		containerResp, err := cli.ContainerCreate(ctx, &container.Config{
			Image:      dstIm,
			Tty:        false,
			Entrypoint: imageSpec.Config.Entrypoint,
			Cmd:        imageSpec.Config.Cmd,
		}, nil, nil, nil, "")
		if err != nil {
			log.Fatalln(err)
		}
		if len(containerResp.Warnings) > 0 {
			log.Println(containerResp.Warnings)
		}

		// copy file from container as tar archive
		fromContainer, _, err := cli.CopyFromContainer(ctx, containerResp.ID, "/disk.qcow2")
		if err != nil {
			log.Fatalln(err)
		}

		// untar file from tar archive
		tarReader := tar.NewReader(fromContainer)
		for {
			header, err := tarReader.Next()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				log.Fatalln(err)
			}

			if header.Name != "disk.qcow2" {
				log.Printf("skip file %s", header.Name)
				continue
			}
			path := filepath.Join(*outFile) //nolint:gocritic // FIXME: this is probably incorrect, intended to join with header.Name ?
			info := header.FileInfo()

			// Open and write to the file inside a closure, so we can defer close
			err = func() error {
				file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
				if err != nil {
					return err
				}
				defer file.Close()
				_, err = io.Copy(file, tarReader)
				return err
			}()
			if err != nil {
				log.Fatalln(err)
			}
		}
		// remove container
		if err = cli.ContainerRemove(ctx, containerResp.ID, types.ContainerRemoveOptions{}); err != nil {
			log.Println(err)
		}

	}
}

type imageSpec struct {
	Commands     []command `yaml:"commands"`
	ShutdownHook string    `yaml:"shutdownHook,omitempty"`
	Build        string    `yaml:"build"`
	Merge        string    `yaml:"merge"`
	Files        []file    `yaml:"files"`
}

type command struct {
	Name           string `yaml:"name"`
	User           string `yaml:"user"`
	SysvInitAction string `yaml:"sysvInitAction"`
	Shell          string `yaml:"shell"`
}

type file struct {
	Filename string  `yaml:"filename"`
	HostPath *string `yaml:"hostPath,omitempty"`
	Content  *string `yaml:"content,omitempty"`
}

func readImageSpec(path string) (*imageSpec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file at %q: %w", path, err)
	}

	var spec imageSpec

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true) // disallow unknown fields
	if err := dec.Decode(&spec); err != nil {
		return nil, err
	}

	var errs []error

	for i, c := range spec.Commands {
		for _, e := range c.validate() {
			errs = append(errs, fmt.Errorf("error in commands[%d]: %w", i, e))
		}
	}
	for i, f := range spec.Files {
		for _, e := range f.validate() {
			errs = append(errs, fmt.Errorf("error in files[%d]: %w", i, e))
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("invalid image spec: %w", err)
	}

	return &spec, nil
}

func (c command) validate() []error {
	checkNonempty := func(errs *[]error, field string, value string) {
		if value == "" {
			*errs = append(*errs, fmt.Errorf("command must have non-empty field '%s'", field))
		}
	}

	var errs []error

	checkNonempty(&errs, "name", c.Name)
	checkNonempty(&errs, "user", c.User)
	checkNonempty(&errs, "sysvInitAction", c.SysvInitAction)
	checkNonempty(&errs, "shell", c.Shell)

	return errs
}

func (f file) validate() []error {
	var errs []error

	if f.Filename == "" {
		errs = append(errs, errors.New("file must have non-empty field 'filename'"))
	}

	if f.HostPath == nil && f.Content == nil {
		errs = append(errs, errors.New("file missing either 'hostPath' or 'content'"))
	} else if f.HostPath != nil && f.Content != nil {
		errs = append(errs, errors.New("file must have only one of 'hostPath' or 'content'"))
	}

	return errs
}

// getAgettyTTY returns the tty device name for agetty based on the target architecture.
func getAgettyTTY(targetArch string) string {
	switch targetArch {
	case targetArchLinuxAmd64:
		return "ttyS0"
	case targetArchLinuxArm64:
		return "ttyAMA0"
	default:
		log.Fatalf("Unsupported target architecture: %q", targetArch)
		return ""
	}
}

func getImageSha(targetArch string, shaAmd64 string, shaArm64 string) string {
	switch targetArch {
	case targetArchLinuxAmd64:
		return shaAmd64
	case targetArchLinuxArm64:
		return shaArm64
	default:
		log.Fatalf("Unsupported target architecture: %q", targetArch)
		return ""
	}
}
