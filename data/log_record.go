package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id
	Offset uint64 // 数据在文件中的位置
}

// 将数据记录编码为字节数组并返回长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, uint64) {
	return nil, 0
}
