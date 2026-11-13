package vmguard

import (
	"context"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

// BirthSourceNames仅复用原出生绑定和状态来源的既有命名算法，供不同客户端
// 进行同源读回。它不验证来源、不认证身份，也不生成退出或NoStart许可。
func BirthSourceNames(binding journal.Binding) [2]string {
	return [2]string{BindingName(binding), stateName(binding)}
}

// ConfirmBirthSourcesAbsent只确认两个原来源的真实404，不是退出或NoStart
// 证明。失败启动退役还必须独立核全Pod终态和未使用的原VM日志；网络、权限、
// 部分存在、格式损坏或同名替换均不能解释为缺失，更不能从404补造原出生。
func ConfirmBirthSourcesAbsent(ctx context.Context, client coreclient.CoreV1Interface, binding journal.Binding) error {
	if control.ValidateID(binding.Namespace, "namespace") != nil || control.ValidateID(binding.Name, "VM name") != nil || control.ValidateID(string(binding.VMUID), "VM UID") != nil || control.ValidateIncarnation(binding.Incarnation) != nil {
		return fmt.Errorf("invalid original VM birth scope")
	}
	for _, name := range BirthSourceNames(binding) {
		_, err := client.ConfigMaps(binding.Namespace).Get(ctx, name, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			if err != nil {
				return err
			}
			return fmt.Errorf("pre-birth cleanup found an existing Runner source %s", name)
		}
	}
	return nil
}
