// Package localcontrol把受信本地管理决定接到Agent现有owner循环。
// 不监听TCP，不代理普通租户请求。socket与其目录是管理权限边界：只有Agent
// 服务UID及root可信；具有同UID代码执行或Pod exec权限的主体属于管理者。
package localcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
)

const (
	CommandPath = "/dsga/v1/command"
	OwnerPath   = "/dsga/v1/owner"
	maxBody     = 16 * 1024
	maxInFlight = 16
	callTimeout = 130 * time.Second
)

// Registry仅保护当前Session端点映射；锁内不等队列、不取凭据、不读Kube。
// 注销使用指针比较，旧owner的迟到defer不得移除已经注册的新Session。
type Registry struct {
	mu      sync.Mutex
	entries map[string]endpoint
}

// 注册入口只接受正式ControlEndpoint；这个包内接口用于隔离HTTP传输测试，
// 不向平台配置开放“任意完成回调”或替代生产worker的执行器。
type endpoint interface {
	Submit(context.Context, control.CoordinationCommand) (owner.CoordinationResult, error)
	Done() <-chan struct{}
	Binding() (control.Instance, control.Uint64)
}

// OwnerBinding只是本Agent注册端点的发现信息，不能代替Session.Check或最终
// 领域fence。调用者据此选择完整绑定，接管竞争仍由Submit保守拒绝。
type OwnerBinding struct {
	ProtocolVersion uint32           `json:"protocol_version"`
	Instance        control.Instance `json:"instance"`
	OwnerEpoch      control.Uint64   `json:"owner_epoch"`
}

func NewRegistry() *Registry {
	return &Registry{mu: sync.Mutex{}, entries: make(map[string]endpoint)}
}

func (r *Registry) Register(endpoint *owner.ControlEndpoint) (func(), error) {
	if endpoint == nil {
		return nil, fmt.Errorf("nil coordination endpoint")
	}
	instance, _ := endpoint.Binding()
	r.mu.Lock()
	defer r.mu.Unlock()
	if previous := r.entries[instance.VMUID]; previous != nil {
		select {
		case <-previous.Done():
		default:
			return nil, fmt.Errorf("coordination owner already registered")
		}
	}
	r.entries[instance.VMUID] = endpoint
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.entries[instance.VMUID] == endpoint {
			delete(r.entries, instance.VMUID)
		}
	}, nil
}

func (r *Registry) lookup(vmUID string) endpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.entries[vmUID]
}

type Server struct {
	http     *http.Server
	listener *net.UnixListener
}

// ValidateSocketPath只校验配置，不创建目录。路径用规范绝对形式且留出两种
// Unix sun_path的空间；部署方须预建专用目录，不能把socket直接放在共享/tmp。
func ValidateSocketPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 100 || filepath.Base(path) == "." || filepath.Base(path) == string(filepath.Separator) {
		return fmt.Errorf("coordination socket requires a short canonical absolute path")
	}
	return nil
}

func Listen(path string, registry *Registry) (*Server, error) {
	if err := ValidateSocketPath(path); err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, fmt.Errorf("coordination registry is required")
	}
	dir := filepath.Dir(path)
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil || resolved != dir {
		return nil, fmt.Errorf("coordination directory must exist without symlinks")
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm()&0o077 != 0 || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("coordination directory must be private and owned by Agent UID")
	}
	// 专用目录的祖先也不能被非管理者替换。允许root/self持有的sticky
	// 临时目录（Mac验证用），其余组/全局可写祖先一律拒绝。
	for parent := filepath.Dir(dir); ; parent = filepath.Dir(parent) {
		ancestor, err := os.Lstat(parent)
		if err != nil {
			return nil, err
		}
		owner, valid := ancestor.Sys().(*syscall.Stat_t)
		if !valid || !ancestor.IsDir() || (owner.Uid != 0 && owner.Uid != uint32(os.Geteuid())) || (ancestor.Mode().Perm()&0o022 != 0 && ancestor.Mode()&os.ModeSticky == 0) {
			return nil, fmt.Errorf("coordination directory ancestor is replaceable")
		}
		if parent == string(filepath.Separator) {
			break
		}
	}
	// 不删除已存在路径：它可能属于另一个Agent或持有未退出的旧服务。崩溃
	// 留下的socket由管理员确认旧进程退出后移除，不能靠启动超时猜无旧写者。
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	server := &http.Server{
		Handler: newHandler(registry), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: callTimeout + 5*time.Second,
		IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8 * 1024,
	}
	return &Server{http: server, listener: listener}, nil
}

// Run随Agent taskgroup运行。取消先关闭监听/HTTP上下文，Submit不分离后台写；
// 原WAL保留已发送未知。端点在owner的Run返回后才Done，不用HTTP关闭假装join。
func (s *Server) Run(ctx context.Context) error {
	s.http.BaseContext = func(net.Listener) context.Context { return ctx }
	stop := context.AfterFunc(ctx, func() { _ = s.Close() })
	defer stop()
	defer s.Close()
	err := s.http.Serve(s.listener)
	if ctx.Err() != nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Close() error {
	// Listen后其他Agent初始化也可能失败；此时Serve尚未登记listener，
	// 只调http.Close会泄漏socket。两者均关闭，忽略重复close的正常终态。
	httpErr := s.http.Close()
	listenErr := s.listener.Close()
	if httpErr != nil {
		return httpErr
	}
	if errors.Is(listenErr, net.ErrClosed) {
		return nil
	}
	return listenErr
}

func writeError(w http.ResponseWriter, status int, code string) {
	// 不回传底层Kube/HTTP/凭据错误；管理者从原journal及内部状态诊断。
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: code})
}

func newHandler(registry *Registry) http.Handler {
	inFlight := make(chan struct{}, maxInFlight)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == OwnerPath && r.URL.RawPath == "" {
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", http.MethodGet)
				writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
				return
			}
			query, parseErr := url.ParseQuery(r.URL.RawQuery)
			values := query["vm_uid"]
			if parseErr != nil || len(query) != 1 || len(values) != 1 || control.ValidateID(values[0], "VM UID") != nil {
				writeError(w, http.StatusBadRequest, "one_vm_uid_required")
				return
			}
			selected := registry.lookup(values[0])
			if selected == nil {
				writeError(w, http.StatusConflict, "owner_unavailable")
				return
			}
			select {
			case <-selected.Done():
				writeError(w, http.StatusConflict, "owner_unavailable")
				return
			default:
			}
			instance, epoch := selected.Binding()
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(w).Encode(OwnerBinding{ProtocolVersion: 1, Instance: instance, OwnerEpoch: epoch})
			return
		}
		if r.URL.Path != CommandPath || r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" {
			writeError(w, http.StatusNotFound, "unknown_route")
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			writeError(w, http.StatusUnsupportedMediaType, "json_required")
			return
		}
		select {
		case inFlight <- struct{}{}:
			defer func() { <-inFlight }()
		default:
			writeError(w, http.StatusTooManyRequests, "coordination_busy")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusRequestEntityTooLarge, "body_unavailable_or_too_large")
			return
		}
		var command control.CoordinationCommand
		if json.Unmarshal(body, &command) != nil {
			writeError(w, http.StatusBadRequest, "invalid_coordination_command")
			return
		}
		endpoint := registry.lookup(command.Instance.VMUID)
		if endpoint == nil {
			writeError(w, http.StatusConflict, "owner_unavailable")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), callTimeout)
		defer cancel()
		result, err := endpoint.Submit(ctx, command)
		if err != nil {
			if errors.Is(err, owner.ErrControlUnavailable) || errors.Is(err, owner.ErrLeadershipLost) {
				writeError(w, http.StatusConflict, "owner_unavailable")
			} else {
				// 任何错误都可能发生在写后。409/503不能解释为NoEffect，
				// 也不触发服务端自动重放；必须查询原账本/原领域。
				writeError(w, http.StatusServiceUnavailable, "inspect_original_journal")
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(result)
	})
}
