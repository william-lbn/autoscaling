//go:build linux

package resourceguard

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"strconv"
	"sync"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

type arrayNativeServices struct {
	admission     *normalAuthorityAdmission
	guard         *Guard
	source        nativeBirthSource
	split         normalSplitInputs
	birth, arrays *net.UnixListener
	cancel        context.CancelFunc
	workers       sync.WaitGroup
	pinMu         sync.Mutex
	launchPins    map[string]*arrayLaunchPin
}

func arrayFixedListener(path string) (*net.UnixListener, error) {
	// 仅root与可信postgres主组可连接；目录仍由root独占写。连接权不作授权。
	account, err := user.Lookup("postgres")
	if err != nil {
		return nil, err
	}
	gid, err := strconv.Atoi(account.Gid)
	if err != nil || gid <= 0 {
		return nil, fmt.Errorf("trusted postgres socket group unavailable")
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err == nil {
		if st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFSOCK {
			return nil, fmt.Errorf("foreign native socket path")
		}
		if err = os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		return nil, err
	}
	if err = os.Chown(path, 0, gid); err != nil {
		listener.Close()
		return nil, err
	}
	if err = os.Chmod(path, 0660); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

// 唯一固定生产构造器在原server.lock内调用，不接受路径/Expected/ready布尔。
func startArrayNativeServices(parent context.Context, g *Guard, sealed SourceConfig) (*arrayNativeServices, error) {
	return startArrayNativeServicesWithAdmission(parent, g, sealed, nil)
}
func startArrayNativeServicesWithAdmission(parent context.Context, g *Guard, sealed SourceConfig, admission *normalAuthorityAdmission) (*arrayNativeServices, error) {
	if admission != nil && admission.guard != g {
		return nil, ErrUnknown
	}
	source, pg, err := readNativeBirthOriginalSource()
	if err != nil {
		return nil, err
	}
	pg.Close()
	if source.resource.Config != g.config || source.resource.ControlPublicKey != sealed.ControlPublicKey || validateNativeArrayHelperPolicy(source.binding.Policy, true) != nil {
		return nil, fmt.Errorf("native services original source differs")
	}
	split, err := g.normalSplitRuntimeInputs(parent)
	if err != nil {
		return nil, err
	}
	birth, err := arrayFixedListener(fixedArrayBirthSocketPath)
	if err != nil {
		return nil, err
	}
	arrays, err := arrayFixedListener(fixedArrayPopulateSocketPath)
	if err != nil {
		birth.Close()
		return nil, err
	}
	if _, err = publishArrayBirthConsumer(source); err != nil {
		birth.Close()
		arrays.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	s := &arrayNativeServices{admission: admission, guard: g, source: source, split: split, birth: birth, arrays: arrays, cancel: cancel, launchPins: map[string]*arrayLaunchPin{}}
	s.workers.Add(2)
	go s.serve(ctx, birth, true)
	go s.serve(ctx, arrays, false)
	return s, nil
}
func (s *arrayNativeServices) close() {
	s.cancel()
	s.birth.Close()
	s.arrays.Close()
	s.workers.Wait()
	s.pinMu.Lock()
	for _, pin := range s.launchPins {
		pin.close()
	}
	s.launchPins = nil
	s.pinMu.Unlock()
}
func (s *arrayNativeServices) serve(ctx context.Context, listener *net.UnixListener, birth bool) {
	defer s.workers.Done()
	for ctx.Err() == nil {
		_ = listener.SetDeadline(time.Now().Add(250 * time.Millisecond))
		conn, err := listener.AcceptUnix()
		if err != nil {
			if n, ok := err.(net.Error); ok && n.Timeout() {
				continue
			}
			if s.admission != nil && ctx.Err() == nil {
				s.admission.fail()
			}
			return
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		if s.admission != nil {
			kind := uint32(2)
			if birth {
				kind = 1
			}
			handled, e := s.admission.native(call, s.guard, conn, kind)
			if handled || e != nil {
				cancel()
				_ = conn.Close()
				continue
			}
		}
		if birth {
			_ = s.handleBirth(call, conn)
		} else {
			_ = s.handleArrays(call, conn)
		}
		cancel()
		_ = conn.Close()
	}
}
func arrayServicePacket(conn *net.UnixConn, deadline time.Time, size int, peek bool) ([]byte, error) {
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size+1)
	oob := make([]byte, unix.CmsgSpace(16*4))
	var n, controlBytes, flags int
	var received error
	requestFlags := unix.MSG_DONTWAIT | unix.MSG_CMSG_CLOEXEC
	if peek {
		requestFlags |= unix.MSG_PEEK
	}
	err = raw.Read(func(fd uintptr) bool {
		for {
			n, controlBytes, flags, _, received = unix.Recvmsg(int(fd), data, oob, requestFlags)
			if received == unix.EINTR {
				continue
			}
			return received != unix.EAGAIN && received != unix.EWOULDBLOCK
		}
	})
	if controlBytes > 0 {
		messages, parseErr := unix.ParseSocketControlMessage(oob[:controlBytes])
		if parseErr == nil {
			for _, message := range messages {
				if message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_RIGHTS {
					fds, e := unix.ParseUnixRights(&message)
					if e == nil {
						for _, fd := range fds {
							_ = unix.Close(fd)
						}
					}
				}
			}
		}
		return nil, fmt.Errorf("native service ancillary forbidden")
	}
	if err != nil {
		return nil, err
	}
	if received != nil {
		return nil, received
	}
	if n != size || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return nil, fmt.Errorf("native service exact packet missing/truncated")
	}
	return data[:n], nil
}
func (s *arrayNativeServices) handleBirth(ctx context.Context, conn *net.UnixConn) error {
	deadline, _ := ctx.Deadline()
	magic, err := arrayServicePeekMagic(conn, deadline)
	if err != nil {
		return err
	}
	if magic == arrayPreexecBeginMagic {
		return s.handlePreexecBegin(ctx, conn)
	}
	if magic == arrayPreexecMagic {
		return s.handlePreexec(ctx, conn)
	}
	if magic != nativeBirthWireMagic {
		return fmt.Errorf("unknown native birth action")
	}
	peek, err := arrayServicePacket(conn, deadline, nativeBirthRequestBytes, true)
	if err != nil {
		return err
	}
	if string(peek[:8]) != nativeBirthWireMagic {
		return fmt.Errorf("native birth magic invalid")
	}
	nonce := hex.EncodeToString(peek[16:48])
	return s.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		pendingRaw, found, err := state.readPhysicalFile(nativeBirthStageName(nonce, "pending"))
		if err != nil || !found {
			return errors.Join(ErrUnknown, fmt.Errorf("native original Pending unavailable"), err)
		}
		var pending nativeBirthPending
		if control.UnmarshalObject(pendingRaw, &pending) != nil || pending.validate() != nil || pending.Nonce != nonce || pending.Source != s.source.binding {
			return errors.Join(ErrUnknown, fmt.Errorf("native peer cannot construct original Source"))
		}
		s.pinMu.Lock()
		launch := s.launchPins[nonce]
		s.pinMu.Unlock()
		if launch == nil || launch.pending != pending {
			return errors.Join(ErrUnknown, fmt.Errorf("native original pre-exec Task pin missing"))
		}
		ref := nativeBirthRef(pendingRaw, pending)
		source, pg, err := readNativeBirthOriginalSource()
		if err != nil {
			return err
		}
		defer pg.Close()
		stage, err := openNativeBirthPendingLocked(state, r, source, ref)
		if err != nil {
			return err
		}
		expected, err := pending.Source.expected(pending.Nonce, pending.Original.AddressSpaceMaxBytes)
		if err != nil {
			return err
		}
		binarySHA, err := nativeBirthDigest(pending.Source.BinarySHA256)
		if err != nil {
			return err
		}
		cfg := nativeBirthTraceConfig{Original: pending.Original, PeerUID: pending.PeerUID, PeerGID: pending.PeerGID, Expected: expected, AuditBinary: pg, BinarySHA256: binarySHA}
		_, original, err := runArrayRegistryBirthTraceSession(ctx, conn, cfg, stage, s.guard, s.split, launch)
		if err != nil {
			return err
		}
		if err = bindArrayRegistryGuardLocked(s.guard, state, r, original); err != nil {
			original.close()
			return err
		}
		return nil
	})
}
