package runnercpu

import control "github.com/neondatabase/autoscaling/pkg/api/dsga"

// ValidateObservationNonce只接受32位规范小写hex，复用原严格nonce约束。
func ValidateObservationNonce(nonce string) error { return control.ValidateIncarnation(nonce) }
