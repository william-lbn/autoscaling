//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"runtime"
)

// 只在原initial-boot的Split/Provision/Check全部成功且原Host端点关闭后调用。
// v3只是显式启动选择；必须再捕原System/FS及真实broker移交，不能凭版本签normal。
func startInitialNormalSystemV3Broker(ctx context.Context, g *Guard) error {
	if ctx == nil || ctx.Err() != nil || g == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("initial v3 broker original context missing"))
	}
	f, e := nativeBirthOpenRO(NormalSystemSourceManifestPath)
	if e != nil {
		return e
	}
	raw, e := nativeBirthReadBounded(f, 1<<20)
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	manifest, e := DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		return e
	}
	if manifest.ProtocolVersion != 3 {
		return nil
	}
	system, e := inspectNormalSystemLifecycleOriginal(ctx)
	if e != nil {
		return e
	}
	defer system.close()
	if system.manifest.ProtocolVersion != 3 {
		return errors.Join(ErrUnknown, fmt.Errorf("initial original System v3 selection changed"))
	}
	filesystem, e := g.captureNormalFilesystemOrigin(ctx, system)
	if e != nil {
		return e
	}
	defer filesystem.close()
	handle, e := startNormalSystemRoleBroker(ctx, g, system, filesystem)
	if e != nil {
		return e
	}
	// 成功的handle不能cancelAndWait：原child在耐久COMMIT后由PID1收养。
	// 父进程正常退出前保持原clone对象存活，子进程已持其独立4FD与typedFS来源。
	runtime.KeepAlive(handle)
	return nil
}
