package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplayRejectsConflictingOrUnboundedInput(t *testing.T) {
	cases := map[string]string{
		"duplicate": `{"protocol_version":1,"protocol_version":2}`,
		"extra":     `{"protocol_version":1,"unknown":true}`,
		"trailing":  `{"protocol_version":1} {}`,
		"too large": strings.Repeat(" ", (1<<20)+1),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			require.Error(t, run(strings.NewReader(raw), &out))
			require.Empty(t, out.Bytes())
		})
	}
}
