//go:build linux && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func normalAuthorityDEVKey() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x39}, 32))
}
func normalAuthorityDEVToken(a *normalSystemResourceAuthority, epoch uint64) string {
	key := normalAuthorityDEVKey()
	pub := key.Public().(ed25519.PublicKey)
	sum := sha256.Sum256(pub)
	header, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": hex.EncodeToString(sum[:16])})
	now := uint64(time.Now().Unix())
	claims := resourceClaims{Audience: "neon-dsga", Scope: "compute_ctl:dsga", ComputeID: "fixture-compute", VMUID: a.origin.ResourceIdentity.VMUID, ComputeInstanceID: a.origin.ResourceIdentity.ComputeInstanceID, Expires: now + 60, NotBefore: now - 1}
	claims.IssuerEpoch = control.Uint64(epoch)
	raw, _ := json.Marshal(claims)
	enc := base64.RawURLEncoding.EncodeToString
	unsigned := enc(header) + "." + enc(raw)
	return unsigned + "." + enc(ed25519.Sign(key, []byte(unsigned)))
}
func normalAuthorityDEVRequest(ctx context.Context, a *normalSystemResourceAuthority, method, path string, body []byte, epoch uint64) (int, []byte, error) {
	transport := &http.Transport{DialContext: func(c context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(c, "unix", FixedSocketPath)
	}, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	request, e := http.NewRequestWithContext(ctx, method, "http://original-authority"+path, bytes.NewReader(body))
	if e != nil {
		return 0, nil, e
	}
	request.Header.Set("Authorization", "Bearer "+normalAuthorityDEVToken(a, epoch))
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, e := (&http.Client{Transport: transport}).Do(request)
	if e != nil {
		return 0, nil, e
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	return response.StatusCode, raw, e
}

// 在真实构造器probe后、prepared之前执行；只发请求/核原状态，不能开启gate。
func normalAuthorityDEVCandidate(ctx context.Context, a *normalSystemResourceAuthority) error {
	if a == nil || a.instance == nil || a.admission.businessAllowed() || a.instance.guard != a.guard || a.instance.handler.guard != a.guard || a.instance.native.guard != a.guard {
		return ErrUnknown
	}
	before, e := os.ReadFile(filepath.Join(a.guard.config.StateDir, "state.json"))
	if e != nil {
		return e
	}
	fence, _ := json.Marshal(map[string]any{"protocol_version": 1, "identity": a.origin.ResourceIdentity, "issuer_epoch": "2"})
	status, _, e := normalAuthorityDEVRequest(ctx, a, http.MethodPost, "/dsg/v1/resource/fence", fence, 2)
	if e != nil || status != http.StatusServiceUnavailable {
		return fmt.Errorf("DEV candidate valid resource business was not rejected")
	}
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return e
	}
	nonce, e := nativeBirthDigest(consumer.InstanceNonce)
	if e != nil {
		return e
	}
	runtime, e := nativeBirthDigest(a.instance.native.source.binding.Policy.RuntimeDocumentSHA256)
	if e != nil {
		return e
	}
	raw := make([]byte, arrayPreexecBytes)
	copy(raw, arrayPreexecBeginMagic)
	binary.BigEndian.PutUint32(raw[8:12], 2)
	binary.BigEndian.PutUint32(raw[12:16], 1)
	copy(raw[16:48], nonce[:])
	copy(raw[48:80], runtime[:])
	for _, path := range []string{fixedArrayBirthSocketPath, fixedArrayPopulateSocketPath} {
		c, e := (&net.Dialer{}).DialContext(ctx, "unixpacket", path)
		if e != nil {
			return e
		}
		conn := c.(*net.UnixConn)
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, e = conn.Write(raw)
		if e != nil {
			conn.Close()
			return e
		}
		var b [1]byte
		n, e := conn.Read(b[:])
		conn.Close()
		if n != 0 || e == nil {
			return fmt.Errorf("DEV candidate native business was not closed")
		}
	}
	after, e := os.ReadFile(filepath.Join(a.guard.config.StateDir, "state.json"))
	if e != nil || !bytes.Equal(before, after) {
		return fmt.Errorf("DEV candidate business changed original resource WAL")
	}
	a.instance.native.pinMu.Lock()
	pins := len(a.instance.native.launchPins)
	a.instance.native.pinMu.Unlock()
	if pins != 0 {
		return fmt.Errorf("DEV candidate created native launch state")
	}
	fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-008 PASS candidate HTTP valid business and original Begin frame rejected on all three actual listeners; no state/Birth/launchPin effects")
	return nil
}
func normalAuthorityDEVMarker(id string) error {
	f, e := os.OpenFile("/run/neon-dsga/dev-exit-"+id, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = f.Write([]byte("confirmed\n")); e != nil {
		return e
	}
	return f.Sync()
}
func normalAuthorityDEVObserve(ctx context.Context) {
	deadline := time.Now().Add(90 * time.Second)
	leaseObserved, attachmentObserved, fenced := false, false, false
	for time.Now().Before(deadline) {
		normalSystemAuthorityIndex.Lock()
		var authority *normalSystemResourceAuthority
		for _, a := range normalSystemAuthorityIndex.byGuard {
			if a != nil && !a.admission.failed.Load() {
				authority = a
				break
			}
		}
		normalSystemAuthorityIndex.Unlock()
		if authority != nil && authority.admission.businessAllowed() {
			a := authority
			// 真COMMIT后的合法签名请求先推进原floor；后继lease/重生必须容许
			// 同一资源记录的正常进展，不能继续锁住precommit整record摘要。
			if !fenced {
				if a.origin.ResourceFloor == control.Uint64(^uint64(0)) {
					fmt.Println("ROLE_AUTHORITY_DEV_FENCE_REFUSED overflow")
					return
				}
				target := a.origin.ResourceFloor + 1
				body, e := json.Marshal(FenceRequest{ProtocolVersion: 1, Identity: a.origin.ResourceIdentity, IssuerEpoch: target})
				if e != nil {
					fmt.Println("ROLE_AUTHORITY_DEV_FENCE_REFUSED encode")
					return
				}
				status, _, e := normalAuthorityDEVRequest(ctx, a, http.MethodPost, "/dsg/v1/resource/fence", body, uint64(target))
				if e != nil || status != http.StatusOK {
					fmt.Println("ROLE_AUTHORITY_DEV_FENCE_REFUSED status", status, e)
					return
				}
				status, raw, e := normalAuthorityDEVRequest(ctx, a, http.MethodGet, "/dsg/v1/resource/state", nil, uint64(target))
				var view View
				if e != nil || status != http.StatusOK || json.Unmarshal(raw, &view) != nil || view.Identity != a.origin.ResourceIdentity || view.IssuerFloorEpoch != target || !view.BootstrapApplied || !view.SystemPlacementObserved {
					fmt.Println("ROLE_AUTHORITY_DEV_FENCE_REFUSED state")
					return
				}
				disk, e := os.ReadFile(filepath.Join(a.guard.config.StateDir, "state.json"))
				var persisted record
				if e != nil || json.Unmarshal(disk, &persisted) != nil || persisted.Identity != a.origin.ResourceIdentity || persisted.Floor != target {
					fmt.Println("ROLE_AUTHORITY_DEV_FENCE_REFUSED persisted")
					return
				}
				fenced = true
			}
			leaseOK, attachOK := false, false
			e := a.guard.withRecord(ctx, func(r *record, state *lockedState) error {
				if !leaseObserved {
					lease, e := a.guard.normalSystemOriginalComputeLeaseLocked(ctx, state, r)
					if e == nil {
						defer lease.close()
						if !fenced || r.Floor != a.origin.ResourceFloor+1 || lease.authority != a || lease.stopped.Role != "compute" || lease.task.source.UID != 1000 || a.instance.guard != a.guard || a.instance.handler.guard != a.guard || a.instance.native.guard != a.guard {
							return ErrUnknown
						}
						leaseOK = true
					}
				}
				a.producerMu.Lock()
				defer a.producerMu.Unlock()
				if fenced && r.Floor == a.origin.ResourceFloor+1 && !attachmentObserved && len(a.attachments) >= 2 && a.attachment != nil && !a.attachment.ended {
					current := a.attachment
					for _, old := range a.attachments {
						if old != current && old.ended {
							// 真旧client已结束，真新client已附着；确定性重放迟到旧handler的结束调用。
							if e := a.appendAttachmentLocked(state, old, "ended"); e != nil {
								return e
							}
							if a.attachment != current || current.ended {
								return ErrUnknown
							}
							attachOK = true
							break
						}
					}
				}
				return nil
			})
			if e != nil {
				fmt.Println("ROLE_AUTHORITY_DEV_OBSERVE_REFUSED", e)
				return
			}
			if leaseOK {
				status, _, e := normalAuthorityDEVRequest(ctx, a, http.MethodGet, "/dsg/v1/resource/state", nil, uint64(a.origin.ResourceFloor+1))
				if e != nil || status != http.StatusOK {
					fmt.Println("ROLE_AUTHORITY_DEV_HTTP_REFUSED", status, e)
					return
				}
				if e = normalAuthorityDEVMarker("009"); e != nil {
					return
				}
				fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-009 PASS legal signed fence advanced original durable floor; original clone Compute lease and HTTP/native producers remain on the same Guard")
				leaseObserved = true
			}
			if attachOK {
				if normalAuthorityDEVMarker("010") != nil {
					return
				}
				fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-010 PASS after legal floor progress real resource respawn attaches same Authority and late original handler cannot end the new nonce")
				attachmentObserved = true
			}
			if leaseObserved && attachmentObserved {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	fmt.Println("ROLE_AUTHORITY_DEV_OBSERVE_TIMEOUT")
}

// 声明1调用真实旧ServeGuest后，用真实JWT/UDS请求确认旧R3监听和handler，
// 不从Root marker推测服务已准备，也不产任何normal来源。
func normalAuthorityDEVLegacyObserve() {
	until := time.Now().Add(45 * time.Second)
	for time.Now().Before(until) {
		source, e := sealedRuntimeSource()
		if e == nil {
			holder := &normalSystemResourceAuthority{origin: normalSystemRoleServiceOrigin{ResourceIdentity: Identity{VMUID: source.Config.VMUID, ComputeInstanceID: source.Config.ComputeInstanceID}}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			status, _, e := normalAuthorityDEVRequest(ctx, holder, http.MethodGet, "/dsg/v1/resource/identity", nil, uint64(source.IssuerEpoch))
			cancel()
			if e == nil && status == http.StatusOK {
				if normalAuthorityDEVMarker("011") != nil {
					return
				}
				fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-011 PASS declaration1 actual standalone R3 ServeGuest and authenticated resource HTTP remain compatible")
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("ROLE_AUTHORITY_DEV_LEGACY_HTTP_TIMEOUT")
}
