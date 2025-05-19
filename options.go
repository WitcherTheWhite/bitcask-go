package bitcask_go

type Options struct {
	// 数据存储目录
	DirPath string

	// 数据文件目标大小
	DataFileSize uint64

	// 是否在每次写入后持久化数据文件
	SyncWrites bool
}
