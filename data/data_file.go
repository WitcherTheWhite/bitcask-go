package data

import (
	fio "bitcask-go/file_io"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value")
)

const (
	DataFileNameSuffix string = ".data"
	HintFileName       string = "hint-index"
	MergeFinFileName   string = "merge-fin"
	SeqNoFileName      string = "seq-no"
)

// 数据文件
type DataFile struct {
	FileId    uint32        // 文件 id
	WriteOff  int64         // 写偏移
	IOManager fio.IOManager // io 读写管理
}

func NewDataFile(fileName string, fileId uint32) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IOManager: ioManager,
	}
	return dataFile, nil
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

// 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return NewDataFile(GetDataFileName(dirPath, fileId), fileId)
}

// 打开 hint 索引文件，用于启动时加载索引
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return NewDataFile(fileName, 0)
}

// 标识 merge 完成的文件
func OpenMergeFinFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinFileName)
	return NewDataFile(fileName, 0)
}

// 保存当前事务序列号
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return NewDataFile(fileName, 0)
}

// 在指定位置读取数据记录
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果 header 长度不足 maxLogRecordHeaderSize，直接读取到文件末尾
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取 header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 读到文件末尾，则返回 EOF 错误
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valSize := int64(header.keySize), int64(header.valueSize)
	recordSize := headerSize + keySize + valSize

	// 读出实际的 key/value 数据
	logRecord := &LogRecord{}
	logRecord.Type = header.recordType
	if keySize > 0 || valSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

// 写入字节流
func (df *DataFile) Write(b []byte) error {
	n, err := df.IOManager.Write(b)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
		Type:  LogRecordNormal,
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

// 持久化当前数据文件
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

// 关闭当前数据文件
func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOManager.Read(b, offset)
	return b, err
}
