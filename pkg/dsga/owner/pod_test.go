package owner

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	authentication "k8s.io/api/authentication/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// 本机HTTP夹具执行真实生成的TokenReview/Pod/SA客户端与序列化；不声称它是
// apiserver真实认证或RBAC验收。凭据只在TokenReview body中，API认证头必须
// 保持签发器自己的身份，避免误把Agent Bearer用作中心服务的Kubernetes凭据。
type subjectAPI struct {
	mu      sync.Mutex
	review  *authentication.TokenReview
	pod     *core.Pod
	account *core.ServiceAccount
	calls   []string
	valid   bool
	block   bool
}

func newSubject(t *testing.T) (*subjectAPI, *kubernetes.Clientset) {
	t.Helper()
	f := new(subjectAPI)
	f.valid = true
	f.review = new(authentication.TokenReview)
	f.review.TypeMeta = metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenReview"}
	f.review.Status = authentication.TokenReviewStatus{
		Authenticated: true, Audiences: []string{CredentialAudience}, Error: "",
		User: authentication.UserInfo{
			Username: "system:serviceaccount:agents:autoscaler", UID: "account-uid", Groups: nil,
			Extra: map[string]authentication.ExtraValue{
				"authentication.kubernetes.io/pod-name": {"agent-pod"}, "authentication.kubernetes.io/pod-uid": {"agent-pod-uid"},
			},
		},
	}
	f.pod = new(core.Pod)
	f.pod.Name, f.pod.Namespace, f.pod.UID, f.pod.ResourceVersion = "agent-pod", "agents", "agent-pod-uid", "pod-rv-1"
	f.pod.Spec.NodeName, f.pod.Spec.ServiceAccountName = "node-a", "autoscaler"
	f.pod.Status.Phase = core.PodRunning
	f.account = new(core.ServiceAccount)
	f.account.Name, f.account.Namespace, f.account.UID, f.account.ResourceVersion = "autoscaler", "agents", "account-uid", "sa-rv-1"
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	config := new(rest.Config)
	config.Host, config.BearerToken, config.QPS, config.Burst = server.URL, "broker-kube-identity", 10000, 10000
	client, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)
	return f, client
}

func (f *subjectAPI) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	block := f.block
	f.mu.Unlock()
	if block {
		// 先读完请求，HTTP/1服务端才可观察对端取消；否则夹具自身会阻塞
		// httptest.Close，误把测试夹具的读体问题当作生产超时失效。
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)
	f.valid = f.valid && r.Header.Get("Authorization") == "Bearer broker-kube-identity"
	w.Header().Set("Content-Type", "application/json")
	switch r.Method + " " + r.URL.Path {
	case "POST /apis/authentication.k8s.io/v1/tokenreviews":
		var incoming authentication.TokenReview
		f.valid = f.valid && json.NewDecoder(r.Body).Decode(&incoming) == nil && incoming.Spec.Token == "projected-agent-token" &&
			len(incoming.Spec.Audiences) == 1 && incoming.Spec.Audiences[0] == CredentialAudience
		_ = json.NewEncoder(w).Encode(f.review)
	case "GET /api/v1/namespaces/agents/pods/agent-pod":
		_ = json.NewEncoder(w).Encode(f.pod)
	case "GET /api/v1/namespaces/agents/serviceaccounts/autoscaler":
		_ = json.NewEncoder(w).Encode(f.account)
	default:
		status(w, http.StatusNotFound)
	}
}

func TestPodBoundHolderIdentityRemainsUniquePerProcess(t *testing.T) {
	_, store, client := newAPI(t)
	options := testOptions()
	options.PodUID = "12345678-abcd-1234-abcd-123456789012"
	a, err := New(client, store, options)
	require.NoError(t, err)
	b, err := New(client, store, options)
	require.NoError(t, err)
	require.NotEqual(t, a.holder, b.holder)
	uid, valid := HolderPodUID(a.holder)
	require.True(t, valid)
	require.Equal(t, options.PodUID, uid)
	for _, holder := range []string{"", "dsga-" + strings.Repeat("a", 32), "dsga-" + strings.Repeat("A", 32) + ".pod", "dsga-" + strings.Repeat("z", 32) + ".pod", "dsga-" + strings.Repeat("a", 32) + ".pod.extra", "dsga-" + strings.Repeat("a", 32) + "." + strings.Repeat("a", 91)} {
		_, valid := HolderPodUID(holder)
		require.False(t, valid, holder)
	}
	for _, uid := range []types.UID{"bad.uid", "bad/uid", "bad uid", types.UID(strings.Repeat("a", 91))} {
		options.PodUID = uid
		_, err := New(client, store, options)
		require.Error(t, err)
	}
}

func TestCredentialVerifierBindsReviewedSubjectToCurrentPodVMLeaseAndJournal(t *testing.T) {
	api, store, coord := newAPI(t)
	subject, coreClient := newSubject(t)
	api.mu.Lock()
	api.vm.Status.Node = "node-a"
	api.mu.Unlock()
	verifier, err := NewPodVerifier(coreClient.AuthenticationV1(), coreClient.CoreV1(), coord, "agents", "autoscaler", time.Second, 0)
	require.NoError(t, err)
	options := testOptions()
	options.PodUID = "agent-pod-uid"
	options.LeaseDuration = 30 * time.Second
	elector, err := New(coord, store, options)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		claim := session.Ownership()
		snapshot, err := verifier.Verify(ctx, "projected-agent-token", store, claim)
		require.NoError(t, err)
		require.Equal(t, session.Actor(), snapshot.State().Owner)
		copyVM := snapshot.VirtualMachine()
		copyVM.UID = "mutated-copy"
		copyVM.Annotations["untrusted-owner-mirror"] = "forged"
		require.Equal(t, binding().VMUID, snapshot.VirtualMachine().UID, "returned VM must not mutate the CAS snapshot")
		_, exists := snapshot.VirtualMachine().Annotations["untrusted-owner-mirror"]
		require.False(t, exists)

		// 所有拒绝用例都在真实同一个活跃Session内运行；先保存完整授权对象再
		// 独立改坏一个事实，恢复后必须再次通过，防止靠前一用例残留失败凑数。
		tests := []struct {
			name   string
			mutate func()
		}{
			{"unauthenticated", func() { subject.review.Status.Authenticated = false }},
			{"review-error", func() { subject.review.Status.Error = "secret-provider-detail" }},
			{"wrong-audience", func() { subject.review.Status.Audiences = []string{"kubernetes.default"} }},
			{"extra-audience", func() { subject.review.Status.Audiences = append(subject.review.Status.Audiences, "other") }},
			{"wrong-account", func() { subject.review.Status.User.Username = "system:serviceaccount:agents:other" }},
			{"recreated-account", func() { subject.account.UID = "account-uid-new" }},
			{"reviewed-other-pod", func() {
				subject.review.Status.User.Extra["authentication.kubernetes.io/pod-uid"] = authentication.ExtraValue{"another-pod"}
			}},
			{"unbound-token", func() { delete(subject.review.Status.User.Extra, "authentication.kubernetes.io/pod-uid") }},
			{"duplicate-pod-claim", func() {
				subject.review.Status.User.Extra["authentication.kubernetes.io/pod-name"] = authentication.ExtraValue{"agent-pod", "another"}
			}},
			{"recreated-pod", func() { subject.pod.UID = "new-pod-uid" }},
			{"deleting-pod", func() { now := metav1.Now(); subject.pod.DeletionTimestamp = &now }},
			{"stopped-pod", func() { subject.pod.Status.Phase = core.PodSucceeded }},
			{"wrong-pod-account", func() { subject.pod.Spec.ServiceAccountName = "other" }},
			{"wrong-node-extra", func() {
				subject.review.Status.User.Extra["authentication.kubernetes.io/node-name"] = authentication.ExtraValue{"node-b"}
			}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				subject.mu.Lock()
				beforeReview, beforePod, beforeAccount := subject.review.DeepCopy(), subject.pod.DeepCopy(), subject.account.DeepCopy()
				test.mutate()
				subject.mu.Unlock()
				_, err := verifier.Verify(ctx, "projected-agent-token", store, claim)
				require.ErrorIs(t, err, ErrCredentialsDenied)
				require.NotContains(t, err.Error(), "secret-provider-detail")
				subject.mu.Lock()
				subject.review, subject.pod, subject.account = beforeReview, beforePod, beforeAccount
				subject.mu.Unlock()
				_, err = verifier.Verify(ctx, "projected-agent-token", store, claim)
				require.NoError(t, err)
			})
		}
		for _, test := range []struct {
			name string
			edit func(*Ownership)
		}{
			{"vm-uid", func(c *Ownership) { c.VMUID = "another-vm" }},
			{"incarnation", func(c *Ownership) { c.Incarnation = "another-incarnation" }},
			{"lease-uid", func(c *Ownership) { c.LeaseUID = "recreated-lease" }},
			{"unbound-holder", func(c *Ownership) { c.HolderID = "legacy-holder" }},
			{"self-reported-epoch", func(c *Ownership) { c.IssuerEpoch++ }},
		} {
			t.Run(test.name, func(t *testing.T) {
				bad := claim
				test.edit(&bad)
				_, err := verifier.Verify(ctx, "projected-agent-token", store, bad)
				require.ErrorIs(t, err, ErrCredentialsDenied)
			})
		}
		api.mu.Lock()
		api.vm.Status.Node = "node-b"
		api.mu.Unlock()
		_, err = verifier.Verify(ctx, "projected-agent-token", store, claim)
		require.ErrorIs(t, err, ErrCredentialsDenied, "same credential cannot manage a VM on another node")
		api.mu.Lock()
		api.vm.Status.Node = "node-a"
		api.mu.Unlock()
		verifier.now = func() time.Time { return time.Now().Add(time.Hour) }
		_, err = verifier.Verify(ctx, "projected-agent-token", store, claim)
		require.ErrorIs(t, err, ErrCredentialsDenied, "a stale lease record cannot mint credentials")
		verifier.now = func() time.Time { return time.Now().Add(-time.Hour) }
		_, err = verifier.Verify(ctx, "projected-agent-token", store, claim)
		require.ErrorIs(t, err, ErrCredentialsDenied, "future renewal outside skew must fail closed")
		verifier.now = time.Now
		_, err = verifier.Verify(ctx, "projected-agent-token", store, claim)
		require.NoError(t, err)
		snapshot, err = store.Load(ctx)
		require.NoError(t, err)
		_, err = store.Takeover(ctx, snapshot, session.Actor(), "another-owner")
		require.NoError(t, err)
		_, err = verifier.Verify(ctx, "projected-agent-token", store, claim)
		require.ErrorIs(t, err, ErrCredentialsDenied, "matching lease alone cannot override the persisted journal owner")
		return nil
	})
	require.NoError(t, err)
	subject.mu.Lock()
	defer subject.mu.Unlock()
	require.True(t, subject.valid, "wrong audience/body or confused Kubernetes identity")
}

func TestCredentialVerificationDeadlineAndEarlyRejection(t *testing.T) {
	_, store, coord := newAPI(t)
	subject, client := newSubject(t)
	verifier, err := NewPodVerifier(client.AuthenticationV1(), client.CoreV1(), coord, "agents", "autoscaler", 80*time.Millisecond, time.Second)
	require.NoError(t, err)
	claim := Ownership{Namespace: "testing", VMName: "vm", VMUID: binding().VMUID, Incarnation: binding().Incarnation, LeaseUID: "lease-uid", HolderID: "dsga-" + strings.Repeat("a", 32) + ".agent-pod-uid", IssuerEpoch: 2}
	for _, token := range []string{"", "invalid token", "invalid\nsecret", strings.Repeat("a", 16385)} {
		_, err := verifier.Verify(context.Background(), token, store, claim)
		require.ErrorIs(t, err, ErrCredentialsDenied)
	}
	subject.mu.Lock()
	require.Empty(t, subject.calls, "malformed tokens must not be forwarded")
	subject.block = true
	subject.mu.Unlock()
	started := time.Now()
	_, err = verifier.Verify(context.Background(), "projected-agent-token", store, claim)
	require.ErrorIs(t, err, ErrCredentialsDenied)
	require.Less(t, time.Since(started), time.Second, "token review must have an independent finite deadline")
	for _, header := range []string{"", "Basic secret", "Bearer ", "Bearer a b", "Bearer a\nb"} {
		_, valid := BearerToken(header)
		require.False(t, valid)
	}
	token, valid := BearerToken("Bearer projected-agent-token")
	require.True(t, valid)
	require.Equal(t, "projected-agent-token", token)
}

func TestCredentialVerifierRequiresExplicitSafeConfiguration(t *testing.T) {
	_, _, coord := newAPI(t)
	_, client := newSubject(t)
	for _, test := range []struct {
		namespace string
		account   string
		timeout   time.Duration
		skew      time.Duration
	}{
		{"", "autoscaler", time.Second, 0},
		{"agents", "", time.Second, 0},
		{"agents", "autoscaler", 0, 0},
		{"agents", "autoscaler", time.Minute, 0},
		{"agents", "autoscaler", time.Second, -time.Second},
		{"agents", "autoscaler", time.Second, 6 * time.Second},
	} {
		_, err := NewPodVerifier(client.AuthenticationV1(), client.CoreV1(), coord, test.namespace, test.account, test.timeout, test.skew)
		require.Error(t, err)
	}
	_, err := NewPodVerifier(nil, client.CoreV1(), coord, "agents", "autoscaler", time.Second, 0)
	require.Error(t, err)
}
