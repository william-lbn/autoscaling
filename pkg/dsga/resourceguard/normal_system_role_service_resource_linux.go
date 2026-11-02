//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/net/netutil"
	"golang.org/x/sys/unix"
)

// 实例只保原g；HTTP与native都实际引用它，不能Open第二个Guard补私有producer。
type normalGuestResourceInstance struct {
	trusted   *trustedControllerOwner
	guard     *Guard
	handler   *resourceHTTP
	native    *arrayNativeServices
	listener  net.Listener
	server    *http.Server
	lock      *os.File
	cancel    context.CancelFunc
	admission *normalAuthorityAdmission
	done      chan struct{}
	mu        sync.Mutex
	err       error
	closeOnce sync.Once
}

func prepareNormalGuestResourceInstance(parent context.Context, g *Guard, source SourceConfig, admission *normalAuthorityAdmission) (out *normalGuestResourceInstance, err error) {
	if parent == nil || parent.Err() != nil || g == nil || source.Config != g.config || os.Getuid() != 0 || os.Geteuid() != 0 {
		return nil, fmt.Errorf("original resource instance source differs")
	}
	if admission != nil && admission.guard != g {
		return nil, ErrUnknown
	}
	ctx, cancel := context.WithCancel(parent)
	v := &normalGuestResourceInstance{guard: g, cancel: cancel, admission: admission, done: make(chan struct{})}
	started, success := false, false
	defer func() {
		if !success {
			if !started {
				close(v.done)
			}
			v.close()
		}
	}()
	if err = g.withRecord(ctx, func(r *record, _ *lockedState) error {
		if r.Bootstrap == nil {
			return fmt.Errorf("original resource bootstrap missing")
		}
		return sameBootstrapSource(source, r.Bootstrap.Original)
	}); err != nil {
		return nil, err
	}
	v.handler, err = newResourceHandler(source, g)
	if err != nil {
		return nil, err
	}
	v.handler.arrayProtocolVersion, err = arrayAuthorityRuntimeProtocol(source)
	if err != nil {
		return nil, err
	}
	fd, e := unix.Open(filepath.Join(source.Config.StateDir, "server.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if e != nil {
		return nil, e
	}
	v.lock = os.NewFile(uintptr(fd), "original-resource-service-lock")
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 {
		return nil, fmt.Errorf("invalid root service lock")
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("resource service already active")
	}
	if source.Config.ExecutionModel == TrustedControllerExecutionModel {
		if admission != nil || v.handler.arrayProtocolVersion != 1 {
			return nil, ErrUnknown
		}
		v.trusted, err = newTrustedControllerOwner(ctx, g, source, v.lock)
		if err != nil {
			return nil, err
		}
		// 预算恢复失败不签资格，正常PG仍可启动；每个normal请求独立保持Unknown。
		if budgetErr := g.prepareNormalQuotaBudgetSource(ctx); budgetErr != nil {
			log.Printf("trusted-controller original normal budget unavailable: %v", budgetErr)
		}
	}
	if err = rootDirectory(filepath.Dir(FixedSocketPath), 0755); err != nil {
		return nil, err
	}
	if v.handler.arrayProtocolVersion == 1 {
		v.native, err = startArrayNativeServicesWithOwner(ctx, g, source, admission, v.trusted)
		if err != nil {
			return nil, err
		}
	} else if admission != nil {
		return nil, fmt.Errorf("broker-owned authority requires original native array capability")
	}
	if err = unix.Lstat(FixedSocketPath, &st); err == nil {
		if st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFSOCK {
			return nil, fmt.Errorf("foreign resource socket path")
		}
		if err = os.Remove(FixedSocketPath); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	raw, e := net.ListenUnix("unix", &net.UnixAddr{Name: FixedSocketPath, Net: "unix"})
	if e != nil {
		return nil, e
	}
	v.listener = raw
	if err = os.Chmod(FixedSocketPath, 0666); err != nil {
		return nil, err
	}
	var handler http.Handler = v.handler
	if admission != nil {
		handler = admission.httpHandler(handler)
		v.listener = &normalAuthorityHTTPListener{UnixListener: raw, slots: make(chan struct{}, 16), done: make(chan struct{})}
	} else {
		v.listener = netutil.LimitListener(raw, 16)
	}
	v.server = &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 12 * 1024}
	if admission != nil {
		v.server.ConnContext = normalAuthorityConnContext
	}
	started = true
	go func() {
		e := v.server.Serve(v.listener)
		if errors.Is(e, http.ErrServerClosed) {
			e = ctx.Err()
		}
		v.mu.Lock()
		v.err = e
		v.mu.Unlock()
		if admission != nil {
			admission.fail()
		}
		close(v.done)
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = v.server.Close()
		case <-v.done:
		}
	}()
	success = true
	return v, nil
}
func (v *normalGuestResourceInstance) close() {
	if v == nil {
		return
	}
	v.closeOnce.Do(func() {
		v.cancel()
		if v.server != nil {
			_ = v.server.Close()
		}
		if v.listener != nil {
			_ = v.listener.Close()
		}
		if v.native != nil {
			v.native.close()
		}
		<-v.done
		if v.trusted != nil {
			v.trusted.close()
		}
		if v.lock != nil {
			unix.Flock(int(v.lock.Fd()), unix.LOCK_UN)
			v.lock.Close()
		}
	})
}
func (v *normalGuestResourceInstance) wait() error {
	if v == nil {
		return ErrUnknown
	}
	<-v.done
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.err
}
func (v *normalGuestResourceInstance) probe(ctx context.Context) error {
	if v == nil || v.admission == nil || v.guard == nil || v.handler.guard != v.guard || v.native == nil || v.native.guard != v.guard {
		return ErrUnknown
	}
	return v.admission.probe(ctx, FixedSocketPath, []string{fixedArrayBirthSocketPath, fixedArrayPopulateSocketPath})
}
