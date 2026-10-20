// Package owner 使用真实Kubernetes Lease选择DSGA协调者，并将其唯一进程身份
// 条件写入原journal单调epoch。Lease不是最终执行fence：PG/Runner仍须独立拒绝
// 旧代次，任何超时/换owner都不抹去已派发未知动作或撤销仍可能被消费的预算。
package owner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

var (
	ErrLeadershipLost = errors.New("DSGA leadership was lost; reconcile original effects")
	ErrWorkerPanic    = errors.New("DSGA owner worker panicked; original effects remain unconfirmed")
)

type Options struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

func DefaultOptions() Options {
	return Options{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 2 * time.Second}
}

// Elector每次New生成随机进程身份，不能将Pod名或调用方自报holder当领导权。
// Run是一次性生命周期；失去租约/返回后必须新建实例，不复用旧Session或epoch。
type Elector struct {
	client  coordclient.LeaseInterface
	store   *journal.Store
	binding journal.Binding
	options Options
	holder  string
	mu      sync.Mutex
	used    bool
}

func New(client coordclient.CoordinationV1Interface, store *journal.Store, options Options) (*Elector, error) {
	if client == nil || store == nil || options.LeaseDuration < time.Second || options.LeaseDuration > 24*time.Hour ||
		options.LeaseDuration%time.Second != 0 || options.RenewDeadline <= 0 || options.RetryPeriod <= 0 ||
		options.LeaseDuration <= options.RenewDeadline || options.RetryPeriod >= options.RenewDeadline || options.RenewDeadline <= time.Duration(leaderelection.JitterFactor*float64(options.RetryPeriod)) {
		return nil, fmt.Errorf("invalid DSGA owner clients or lease timings")
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return nil, fmt.Errorf("DSGA process identity entropy unavailable")
	}
	binding := store.Binding()
	return &Elector{client: client.Leases(binding.Namespace), store: store, binding: binding, options: options, holder: "dsga-" + hex.EncodeToString(entropy[:]), mu: sync.Mutex{}, used: false}, nil
}

// Session只能由选主成功且journal接管已确认后创建。Actor只是值副本，不是
// 独立许可；每次派发/签发凭据前仍须Check，原生PG与最终Runner还要执行fence。
type Session struct {
	ctx        context.Context
	client     coordclient.LeaseInterface
	store      *journal.Store
	binding    journal.Binding
	leaseUID   types.UID
	actor      journal.Owner
	apiTimeout time.Duration
}

func (session *Session) Actor() journal.Owner     { return session.actor }
func (session *Session) Context() context.Context { return session.ctx }

// Check直接读当前Lease及journal，验证TLS/RBAC客户端返回的固定UID/holder/
// incarnation/epoch，不从调用方数字、远端墙钟或本地缓存IsLeader推导最终许可。
// 两对象不能原子读取，返回后也可能换主；因此这只限制当前协调循环，不能替代
// PG/Runner串行fence。取消立即阻止新调用，已派发动作仍须持久对账。
func (session *Session) Check(ctx context.Context) error {
	if session == nil || session.ctx == nil || session.ctx.Err() != nil {
		return ErrLeadershipLost
	}
	// VM读可能单独卡住而Lease仍健康续约。检查必须有自己的期限，不能只等
	// 失租取消，更不能让取凭据回调无限挂在HTTP Client.Do之前。
	callCtx, cancel := context.WithTimeout(ctx, session.apiTimeout)
	stop := context.AfterFunc(session.ctx, cancel)
	defer stop()
	defer cancel()
	if session.ctx.Err() != nil {
		return ErrLeadershipLost
	}
	lease, err := session.client.Get(callCtx, LeaseName(session.binding), metav1.GetOptions{})
	if err != nil {
		return err
	}
	if validateLease(lease, session.binding, LeaseName(session.binding), session.leaseUID) != nil || *lease.Spec.HolderIdentity != session.actor.HolderID {
		return ErrLeadershipLost
	}
	snapshot, err := session.store.Load(callCtx)
	if err != nil {
		return err
	}
	if snapshot.State().Owner != session.actor || session.ctx.Err() != nil {
		return ErrLeadershipLost
	}
	return nil
}

// claim不初始化空日志，也不从LeaseTransitions构造epoch。持久单调epoch的
// 唯一来源是原journal的CAS Takeover；响应丢失只读取确认本holder的同一决定，
// 不能再加一次代次。API/RV变化仅有界重试，每次重新确认真实Lease仍属于本进程。
func (elector *Elector) claim(ctx context.Context, lock *boundLease) (*Session, error) {
	claimCtx, cancel := context.WithTimeout(ctx, elector.options.RenewDeadline)
	defer cancel()
	for attempt := 0; attempt <= 3; attempt++ {
		if claimCtx.Err() != nil {
			return nil, claimCtx.Err()
		}
		lease, err := elector.client.Get(claimCtx, lock.name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if validateLease(lease, elector.binding, lock.name, lock.fixedUID()) != nil || *lease.Spec.HolderIdentity != elector.holder {
			return nil, ErrLeadershipLost
		}
		snapshot, err := elector.store.Load(claimCtx)
		if err != nil {
			return nil, err
		}
		if snapshot.State().Owner.HolderID == elector.holder {
			// 长期Session沿用领导权ctx，不能继承即将结束的短期claimCtx。
			session := &Session{ctx: ctx, client: elector.client, store: elector.store, binding: elector.binding, leaseUID: lease.UID, actor: snapshot.State().Owner, apiTimeout: elector.options.RenewDeadline}
			if err := session.Check(claimCtx); err != nil {
				return nil, err
			}
			return session, nil
		}
		if attempt == 3 {
			break
		}
		_, err = elector.store.Takeover(claimCtx, snapshot, snapshot.State().Owner, elector.holder)
		if err != nil {
			var unknown *journal.UnconfirmedWrite
			if !errors.As(err, &unknown) {
				return nil, err
			}
		}
		// 包括成功和未知写都重读确认：后续才允许构造Session/签发当前epoch。
		// 最后一次仍须确认已成功提交，避免把本地expected当成远端事实。
	}
	return nil, fmt.Errorf("DSGA journal ownership was not confirmed after bounded attempts")
}

type workerFailure struct{ cause error }

func (err *workerFailure) Error() string {
	return "DSGA owner worker failed; reconcile original effects"
}
func (err *workerFailure) Unwrap() error { return err.cause }

// Run注册业务回调前先确认Lease和journal，失租/取消后等待已注册回调真正退出。
// client-go回调异步且OnStopped也可能先于OnStarted执行，不能在异步回调里Add后
// 直接Wait：会漏掉迟到回调。registration锁封闭注册窗口，停止后迟到回调无业务
// 副作用；锁内只登记，不做网络或等待。ReleaseOnCancel=false，避免尚未查明的
// SQL/VM效果因提前放Lease而被误判撤销；新owner仍需跨层fence及原预算对账。
func (elector *Elector) Run(ctx context.Context, worker func(context.Context, *Session) error) error {
	if worker == nil {
		return fmt.Errorf("DSGA owner worker is required")
	}
	elector.mu.Lock()
	if elector.used {
		elector.mu.Unlock()
		return fmt.Errorf("DSGA elector cannot be reused")
	}
	elector.used = true
	elector.mu.Unlock()
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	lock := &boundLease{client: elector.client, binding: elector.binding, holder: elector.holder, name: LeaseName(elector.binding), lease: nil, mu: sync.Mutex{}, uid: ""}
	var registration sync.Mutex
	var workers sync.WaitGroup
	stopped := false
	var workerErr error
	normalCompletion := false
	stopRegistration := func() { registration.Lock(); stopped = true; cancel(); registration.Unlock() }
	leader, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: lock, LeaseDuration: elector.options.LeaseDuration, RenewDeadline: elector.options.RenewDeadline, RetryPeriod: elector.options.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				registration.Lock()
				if stopped {
					registration.Unlock()
					return
				}
				workers.Add(1)
				registration.Unlock()
				defer workers.Done()
				defer cancel()
				defer func() {
					if recover() != nil {
						workerErr = ErrWorkerPanic
					}
				}()
				session, err := elector.claim(leaderCtx, lock)
				if err != nil {
					workerErr = err
					return
				}
				if leaderCtx.Err() != nil {
					workerErr = ErrLeadershipLost
					return
				}
				if err := worker(leaderCtx, session); err != nil {
					workerErr = &workerFailure{cause: err}
				} else {
					normalCompletion = leaderCtx.Err() == nil
				}
			},
			OnStoppedLeading: stopRegistration,
		},
		ReleaseOnCancel: false, Name: lock.name,
	})
	if err != nil {
		return err
	}
	leader.Run(loopCtx)
	stopRegistration()
	workers.Wait()
	if workerErr != nil {
		return workerErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if normalCompletion {
		return nil
	}
	return ErrLeadershipLost
}
