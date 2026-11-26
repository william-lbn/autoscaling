//go:build linux

package main

import (
	"fmt"
	"golang.org/x/sys/unix"
)

// CLOCK_BOOTTIME仅供审计。消费者靠本次nonce与原boot身份，不跨boot比较或授TTL许可。
func cpuObservationBootTime() (uint64, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0, err
	}
	if ts.Sec < 0 || ts.Nsec < 0 || ts.Nsec >= 1_000_000_000 || uint64(ts.Sec) > (^uint64(0)-uint64(ts.Nsec))/1_000_000_000 {
		return 0, fmt.Errorf("invalid original CPU kernel boot clock")
	}
	return uint64(ts.Sec)*1_000_000_000 + uint64(ts.Nsec), nil
}
