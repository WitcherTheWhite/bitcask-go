package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"sync"
)

// 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃数据文件
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件
	index      index.Indexer             // 内存索引
}

// 写入 key/value数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 写入磁盘数据文件
	pos, err := db.appendLogRecord(log_record)
	if err != nil {
		return err
	}

	// 更新内存索引信息
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// 根据 key 读取 value 数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 判断 key 是否有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 在内存索引中读取 key 的位置信息
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	// 获取 key 所在的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrDataFileNotFound
	}

	return logRecord.Value, nil
}

// 将数据记录写入到当前活跃文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 存储引擎启动时需要初始化当前活跃文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果超过数据文件目标大小，持久化当前活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件冻结
		db.oldFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	offset := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: offset,
	}
	return pos, nil
}

// 设置当前活跃文件
func (db *DB) setActiveFile() error {
	var fileId uint32
	if db.activeFile != nil {
		fileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, fileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
