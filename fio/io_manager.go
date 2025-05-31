package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	// 标准文件 IO
	StandardIO FileIOType = iota

	// 内存文件映射
	MemoryMap
)

// IO 管理接口，可以支持不同的 IO 类型
type IOManager interface {
	// 从文件指定位置读取数据
	Read([]byte, int64) (int, error)

	// 写入字节流到文件中
	Write([]byte) (int, error)

	// 持久化数据
	Sync() error

	// 关闭文件
	Close() error

	// 获取文件大小
	Size() (int64, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
