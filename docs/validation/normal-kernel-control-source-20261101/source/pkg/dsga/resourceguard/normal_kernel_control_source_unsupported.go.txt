//go:build !linux || !amd64

package resourceguard

// 没有准确amd64/369内核，普通角色控制不能降为metadata-only认证。
func normalKernelControlPlatformSupported() bool { return false }
