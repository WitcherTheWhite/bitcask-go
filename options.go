package bitcask_go

import (
	"bitcask-go/index"
	"os"
)

type Options struct {
	// 数据存储目录
	DirPath string

	// 数据文件目标大小
	DataFileSize int64

	// 是否在每次写入后持久化数据文件
	SyncWrites bool

	// 内存索引类型
	indexType index.IndexType
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	indexType:    index.Btree,
}

type IteratorOptions struct {
	// 遍历前缀值，默认为空
	Prefix []byte

	// 遍历方向
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
