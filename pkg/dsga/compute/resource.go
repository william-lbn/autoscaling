package compute

import (
	"context"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// ResourceIdentity读取独立于PG boot的原Guest资源域。调用方必须把完整身份保存
// 到外层持久日志，再用resourceguard.NewClient创建资源通道；恢复已有未知动作
// 时只能恢复原身份，不能反复调用此方法用新Guest覆盖旧资源承诺。读取成功不
// 表示预算已保留、约束已落实、初始准入可打开或VM可以缩容。
func (client *Client) ResourceIdentity(ctx context.Context) (resourceguard.Identity, error) {
	var id resourceguard.Identity
	if err := client.read(ctx, "/dsg/v1/resource/identity", &id); err != nil {
		return id, err
	}
	if id.VMUID != client.instance.VMUID || id.ComputeInstanceID != client.instance.ComputeInstanceID || id.Validate() != nil {
		return resourceguard.Identity{}, &CallError{Code: "resource_identity_unconfirmed"}
	}
	return id, nil
}
