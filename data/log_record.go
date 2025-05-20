package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// crc type keySize valueSize
// 4 +  1  +  5   +   5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 数据头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // LogRecord的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id
	Offset int64  // 数据在文件中的位置
}

// 将数据记录编码为字节数组并返回长度
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}

func decodeLogRecordHeader(b []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
