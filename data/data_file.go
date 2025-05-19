package data

import (
	fio "bitcask-go/file_io"
)

// 数据文件
type DataFile struct {
	FileId    uint32        // 文件 id
	WriteOff  uint64        // 写偏移
	IOManager fio.IOManager // io 读写管理
}

// 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// 持久化当前数据文件
func (df *DataFile) Sync() error {
	return nil
}

// 写入字节流
func (df *DataFile) Write(bytes []byte) error {
	return nil
}

// 在指定位置读取数据记录
func (df *DataFile) ReadLogRecord(offset uint64) (*LogRecord, error) {
	return nil, nil
}
