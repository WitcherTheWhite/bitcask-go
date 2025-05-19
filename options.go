package bitcask_go

import "bitcask-go/index"

type Options struct {
	// 数据存储目录
	DirPath string

	// 数据文件目标大小
	DataFileSize uint64

	// 是否在每次写入后持久化数据文件
	SyncWrites bool

	// 内存索引类型
	indexType index.IndexType
}
