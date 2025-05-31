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
	IndexType index.IndexType

	// 累计写到阈值后持久化
	BytesPerSync uint

	// 是否在启动时使用 mmap 优化
	MMapAtStartup bool
}

var DefaultOptions = Options{
	DirPath:       os.TempDir(),
	DataFileSize:  256 * 1024 * 1024, // 256MB
	SyncWrites:    false,
	IndexType:     index.BTREE,
	BytesPerSync:  0,
	MMapAtStartup: true,
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

type WriteBatchOptions struct {
	// 一个批次最大的数据量
	MaxBatchSize uint

	// 提交时是否持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 1024,
	SyncWrites:   true,
}
