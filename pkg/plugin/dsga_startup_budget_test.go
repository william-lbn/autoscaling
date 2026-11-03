package plugin

import (
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/stretchr/testify/require"
)

// PG17-U-HOST-BOOT-002：原15s事件预算之外完整覆盖旧Lease观察及最大jitter重试。
func TestDSGAStartupBudgetIncludesHostObservationWithoutChangingLegacy(t *testing.T) {
	config := &Config{StartupEventHandlingTimeoutSeconds: 15}
	require.Equal(t, 15*time.Second, initialEventHandlingTimeout(config), "legacy处理预算不得改变")
	config.DSGAHost = &hostscheduler.Config{}
	observation := hostscheduler.InitialOwnerObservationBudget()
	require.Equal(t, 20*time.Second, observation, "由真实15s Lease及2s Retry/Jitter1.2向上取整")
	require.GreaterOrEqual(t, observation, 15*time.Second+time.Duration(float64(2*time.Second)*(1+1.2)))
	require.Equal(t, 35*time.Second, initialEventHandlingTimeout(config), "DSGA不能在完整旧Lease观察结束前反复超时")
}
