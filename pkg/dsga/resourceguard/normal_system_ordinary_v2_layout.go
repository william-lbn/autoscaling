package resourceguard

// 这两个常量只统一新普通控制2的固定文件位置，不选择模式、不认证镜像或角色。
// Builder必须在最终System库存/原RuntimeClosure摘要形成之前生成实际字节。
const NormalSystemOrdinaryV2AllowMarkerPath = "/run/neon-ordinary/allow-start/vmstart.allowed"
const NormalSystemOrdinaryV2VectorDataPath = "/run/neon-ordinary/logs"

// 路径只来自本固定表，不接受请求path/uid/mode。/run/udev属于Root stock producer，
// 它的Root:60004/01771及rules.d只读合同不能被此表泛chown成普通私有目录。
type normalOrdinaryV2PathSpec struct {
	path, anchor   string
	uid, gid, mode uint32
	file           bool
}

func normalOrdinaryV2Paths() []normalOrdinaryV2PathSpec {
	return []normalOrdinaryV2PathSpec{
		{"/run/neon-ordinary", "/run", 0, 0, 0755, false},
		{"/run/neon-ordinary/allow-start", "/run", 60001, 60001, 0700, false},
		{"/run/neon-ordinary/network", "/run", 60003, 60003, 0700, false},
		{"/run/neon-ordinary/acpi", "/run", 60006, 60006, 0700, false},
		{"/run/neon-ordinary/logs", "/run", 60007, 60007, 0700, false},
		{"/run/chrony", "/run", 60008, 60008, 0700, false},
		{"/var/log/vector", "/var/log", 60007, 60007, 0700, false},
		{"/var/log/chrony", "/var/log", 60008, 60008, 0700, false},
		{"/var/lib/chrony", "/var/lib/chrony", 60008, 60008, 0700, false},
		// DNS仍供PG/其它服务读。只改原独立ext4孔的同一regular inode，不换文件/截断内容。
		{"/etc/resolv.conf", "/etc/resolv.conf", 60003, 60003, 0644, true},
	}
}
