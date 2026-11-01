package resourceguard

import control "github.com/neondatabase/autoscaling/pkg/api/dsga"

func normalKernelControlDecodeObject(raw []byte, out any) error {
	return control.UnmarshalObjectBounded(raw, out, 64<<10, 16384)
}
