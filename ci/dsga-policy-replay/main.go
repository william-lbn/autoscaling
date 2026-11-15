// dsga-policy-replay 从 stdin 读取已冻结的原 Owner 输入，在 stdout 输出逐样本建议。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
)

func run(in io.Reader, out io.Writer) error {
	const maximumBytes = 1 << 20
	raw, err := io.ReadAll(io.LimitReader(in, maximumBytes+1))
	if err != nil {
		return err
	}
	if err := control.ValidateJSON(raw, maximumBytes, 1<<16); err != nil {
		return err
	}
	var trace policy.ReplayTrace
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&trace); err != nil {
		return err
	}
	result, err := policy.Replay(trace)
	if err != nil {
		return err
	}
	encoded := json.NewEncoder(out)
	encoded.SetIndent("", "  ")
	return encoded.Encode(result)
}

func main() {
	if err := run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
