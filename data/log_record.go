package data

import (
	"encoding/binary"
	"hash/crc32"
)

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
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = lr.Type
	index := 5

	index += binary.PutVarint(header[index:], int64(len(lr.Key)))
	index += binary.PutVarint(header[index:], int64(len(lr.Value)))

	size := index + len(lr.Key) + len(lr.Value)
	encBytes := make([]byte, size)
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], lr.Key)
	copy(encBytes[index+len(lr.Key):], lr.Value)

	// 对 LogRecord 数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// 解码得到 LogRecord 的头部信息
func decodeLogRecordHeader(b []byte) (*logRecordHeader, int64) {
	if len(b) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(b[:4]),
		recordType: b[4],
	}

	index := 5

	// 取出 key 长度
	keySize, n := binary.Varint(b[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出 value 长度
	valSize, n := binary.Varint(b[index:])
	header.valueSize = uint32(valSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
