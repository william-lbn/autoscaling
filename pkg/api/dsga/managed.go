package dsga

import (
	"fmt"
	"strings"
)

const (
	JournalAnnotation = "internal.autoscaling.neon.tech/dsga-journal-v1"
	ModeAnnotation    = "internal.autoscaling.neon.tech/dsga-incarnation"
)

// 只按存在性判断，损坏/缺半边的账本亦不能回落到旧无fencing协议。
// 此标识不是能力/认证证明；实际启用仍须完整协商和发布门槛。
func Managed(annotations map[string]string) bool {
	_, journal := annotations[JournalAnnotation]
	_, mode := annotations[ModeAnnotation]
	return journal || mode
}

// 防止普通metadata更新抹掉受管标记后悄悄走旧路径。此检查只保护标记/存在性，
// 不认证账本内容/领导权，不能代替可信单写者和最终Runner的持久fencing。
func ValidateManagedTransition(before, after map[string]string, canInitialize bool) error {
	if !Managed(before) && !Managed(after) {
		return nil
	}
	mode, present := after[ModeAnnotation]
	if !present || !strings.HasPrefix(mode, "1:") || len(mode) != 34 || after[JournalAnnotation] == "" {
		return fmt.Errorf("DSGA mode/journal cannot be missing or malformed")
	}
	nonzero := false
	for _, b := range []byte(mode[2:]) {
		if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
			return fmt.Errorf("invalid DSGA incarnation")
		}
		nonzero = nonzero || b != '0'
	}
	if !nonzero {
		return fmt.Errorf("zero DSGA incarnation")
	}
	if Managed(before) {
		if before[ModeAnnotation] != mode {
			return fmt.Errorf("DSGA incarnation is immutable for this VM")
		}
	} else if !canInitialize {
		return fmt.Errorf("DSGA must initialize before any runner execution")
	}
	return nil
}
