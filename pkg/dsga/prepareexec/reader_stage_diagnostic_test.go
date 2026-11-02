package prepareexec

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/require"
	"log"
	"regexp"
	"strings"
	"testing"
)

// 使用原SDK支持的空快照拒绝路径；不调用外部reader或产生执行许可。
func TestPrepareReaderFailureLogsOnlyFixedScalars(t *testing.T) {
	for _, which := range []string{"missing_snapshot", "inspect_missing_snapshot"} {
		t.Run(which, func(t *testing.T) {
			originalOutput, originalFlags, originalPrefix := log.Writer(), log.Flags(), log.Prefix()
			var output bytes.Buffer
			log.SetOutput(&output)
			log.SetFlags(0)
			log.SetPrefix("")
			t.Cleanup(func() { log.SetOutput(originalOutput); log.SetFlags(originalFlags); log.SetPrefix(originalPrefix) })
			executor := &Executor{}
			var err error
			expectedLines := 1
			if which == "inspect_missing_snapshot" {
				_, err = executor.inspect(context.Background(), nil)
				expectedLines = 2
			} else {
				err = executor.checkSnapshot(context.Background(), nil)
			}
			require.Error(t, err)
			lines := strings.Split(strings.TrimSpace(output.String()), "\n")
			require.Len(t, lines, expectedLines)
			for _, line := range lines {
				require.Regexp(t, regexp.MustCompile(`^DSGA_PREPARE_FAILURE scope=[123] stage=[0-9]+$`), line)
			}
			require.NotContains(t, output.String(), err.Error())
		})
	}
}
