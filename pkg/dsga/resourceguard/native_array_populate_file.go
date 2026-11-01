package resourceguard

import control "github.com/neondatabase/autoscaling/pkg/api/dsga"

// 这里只保留跨平台引用共用的五字段数据形状；不提供FD打开、内核观察或授权能力。
// Linux helper与PG来源引用仍共用同一类型，字段、顺序及原JSON表示逐字不变。
// 四FD必须由原Root真实map_files/原PG boot验证者提供。本私有组件不接受HTTP、
// 租户路径或“verified”bool；仍须后继原Source/完整生命周期消费者接线。
type arrayPopulateFile struct {
	Device    control.Uint64 `json:"device"`
	Inode     control.Uint64 `json:"inode"`
	FileBytes control.Uint64 `json:"file_bytes"`
	Offset    control.Uint64 `json:"offset"`
	Length    control.Uint64 `json:"length"`
}
