package fio

const DataFilePerm = 0644

// IO 管理接口，可以支持不同的 IO 类型
type IOManager interface {
	// 从文件指定位置读取数据
	Read([]byte, int64) (int, error)

	// 写入字节流到文件中
	Write([]byte) (int, error)

	// 持久化数据
	Sync([]byte) error

	// 关闭文件
	Close() error
}
