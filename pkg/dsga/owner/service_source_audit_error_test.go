package owner

import (
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"
)

type sourceAuditLogRow struct {
	message string
	values  []any
}
type sourceAuditLogSink struct {
	mu   sync.Mutex
	rows []sourceAuditLogRow
}

func (s *sourceAuditLogSink) Init(logr.RuntimeInfo) {}
func (s *sourceAuditLogSink) Enabled(int) bool      { return true }
func (s *sourceAuditLogSink) Info(_ int, message string, values ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, sourceAuditLogRow{message: message, values: append([]any(nil), values...)})
}
func (s *sourceAuditLogSink) Error(_ error, message string, values ...any) {
	s.Info(0, message, values...)
}
func (s *sourceAuditLogSink) WithValues(...any) logr.LogSink { return s }
func (s *sourceAuditLogSink) WithName(string) logr.LogSink   { return s }
func (s *sourceAuditLogSink) read() []sourceAuditLogRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sourceAuditLogRow(nil), s.rows...)
}

// 不并行替换进程logger。只检查这两个固定阶段；opaque Error一旦被格式化就panic，
// 确认真实nil不输出、native expiry保安全叶，原error不参与日志的Error字符串渲染。
func TestServiceSourceAuditErrorNilAndOpaqueRemainSafe(t *testing.T) {
	sink := new(sourceAuditLogSink)
	klog.SetLogger(logr.New(sink))
	defer klog.ClearLogger()
	recordServiceSourceAuditError("before_finalization", nil)
	require.Empty(t, sink.read(), "nil审计不制造失败日志")
	require.NotPanics(t, func() { recordServiceSourceAuditError("before_finalization", &privateReopenError{}) })
	require.NotPanics(t, func() { recordServiceSourceAuditError("idle_audit_renew", errNativeAdmissionExpired) })
	rows := sink.read()
	require.Len(t, rows, 2)
	for i, row := range rows {
		require.Equal(t, "DSGA service source audit stopped", row.message)
		require.Len(t, row.values, 6)
		fields := map[string]string{}
		for n := 0; n < len(row.values); n += 2 {
			key, ok := row.values[n].(string)
			require.True(t, ok)
			value, ok := row.values[n+1].(string)
			require.True(t, ok, "原error对象/正文不得进入日志")
			fields[key] = value
		}
		require.ElementsMatch(t, []string{"stage", "error_type", "error_leaf"}, []string{row.values[0].(string), row.values[2].(string), row.values[4].(string)})
		if i == 0 {
			require.Equal(t, "before_finalization", fields["stage"])
			require.Equal(t, "validation_unclassified", fields["error_leaf"])
		} else {
			require.Equal(t, "idle_audit_renew", fields["stage"])
			require.Equal(t, "native_lease_expired", fields["error_leaf"])
		}
	}
}
