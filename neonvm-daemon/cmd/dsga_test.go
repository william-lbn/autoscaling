package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"go.uber.org/zap"
)

// 生产路由/输入拒绝基本检查，不监听TCP，不访问Guest目录，不执行K8s或内核。
// TLS实际正向供给/轮转及权限由后续Linux环境验证，不能用这些拒绝用例替代。
func TestManagedDaemonRejectsLegacyResourceWritesAndNonTLSFiles(t *testing.T) {
	s := &cpuServer{fileOperationsMutex: &sync.Mutex{}, logger: zap.NewNop()}
	app := s.routes(func() bool { return true })
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{http.MethodPut, "/cpu", "2000", 404},
		{http.MethodPut, "/files/etc/neon-dsga", `{"resource.json":{"data":""}}`, 404},
		{http.MethodGet, "/files/etc/sudoers.d", "", 404},
		{http.MethodPut, "/files" + resourceguard.FixedManagementTLSPath, `{"../sudoers":{"data":""}}`, 400},
		{http.MethodPut, "/files" + resourceguard.FixedManagementTLSPath, `{"runtime.json":{"data":""}}`, 400},
		{http.MethodPut, "/files" + resourceguard.FixedManagementTLSPath, `{"tls.crt":{"data":""},"tls.crt":{"data":""}}`, 400},
		{http.MethodPut, "/files" + resourceguard.FixedManagementTLSPath, strings.Repeat("a", 64*1024+1), 400},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s %s: got %d expected %d", tc.method, tc.path, w.Code, tc.status)
		}
	}
}
