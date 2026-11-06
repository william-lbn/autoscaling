package owner

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 真实调用已存在的Active审计：缺固定来源仍逐字返回原错误，新增诊断只记录
// 固定stage/type/leaf。空Snapshot仅在最早固定域检查退出，不发送任何SDK请求。
// logger是进程级，故本测试不Parallel；Opaque/nil安全继续复用既有直接测试。
func TestActiveServiceSourceAuditFailureRecordsFixedStage(t *testing.T) {
	sink := new(sourceAuditLogSink)
	klog.SetLogger(logr.New(sink))
	defer klog.ClearLogger()
	worker := new(PGWorker)
	snapshot := new(journal.Snapshot)
	err := worker.inspectActiveService(context.Background(), snapshot)
	require.EqualError(t, err, "active service lacks fixed original domains", "原return不得包装或替换")
	rows := sink.read()
	require.Len(t, rows, 1, "已有Active审计失败必须产生一条固定stage安全诊断")
	require.Equal(t, "DSGA service source audit stopped", rows[0].message)
	require.Len(t, rows[0].values, 6)
	fields := map[string]string{}
	for i := 0; i < len(rows[0].values); i += 2 {
		key, ok := rows[0].values[i].(string)
		require.True(t, ok)
		value, ok := rows[0].values[i+1].(string)
		require.True(t, ok, "不得将原error或正文送入logger")
		fields[key] = value
	}
	require.Equal(t, "active_domains", fields["stage"])
	require.Equal(t, "validation_unclassified", fields["error_leaf"])
	require.Equal(t, "*errors.errorString", fields["error_type"])
	require.ElementsMatch(t, []string{"stage", "error_type", "error_leaf"}, []string{rows[0].values[0].(string), rows[0].values[2].(string), rows[0].values[4].(string)})
}
