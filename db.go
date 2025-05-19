package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // 文件 id 列表
	activeFile *data.DataFile            // 当前活跃数据文件
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件
	index      index.Indexer             // 内存索引
}

// 打开存储引擎实例
func Open(opts Options) (*DB, error) {
	if err := checkOptions(opts); err != nil {
		return nil, err
	}

	// 数据目录不存在则新建数据目录
	if _, err := os.Stat(opts.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(opts.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		options:  opts,
		mu:       new(sync.RWMutex),
		oldFiles: make(map[uint32]*data.DataFile),
		index:    index.NewIndexer(opts.indexType),
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if err := db.loadIndex(); err != nil {
		return nil, err
	}

	return db, nil
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

	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
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

// 校验配置项
func checkOptions(opts Options) error {
	if opts.DirPath == "" {
		return errors.New("dir path is empty")
	}
	if opts.DataFileSize <= 0 {
		return errors.New("datafile size must be positive")
	}
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	// 找到所有数据文件 id
	var fileIds []int
	for _, dirEntry := range dirEntries {
		if strings.HasSuffix(dirEntry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(dirEntry.Name(), " ")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataFileCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 将文件 id 排序，用于之后加载索引
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 打开所有数据文件
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.oldFiles[uint32(fileId)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
func (db *DB) loadIndex() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有数据文件，并把记录加载到索引
	for _, fid := range db.fileIds {
		fileId := uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.oldFiles[fileId]
		}

		// 循环读取数据文件中记录
		var offset uint64
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 更新索引
			pos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, pos)
			}

			offset += size
		}

		// 如果是活跃文件，更新写入偏移
		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOff = offset
		}
	}

	return nil
}
