package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// 存储引擎实例
type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIds         []int                     // 文件 id 列表
	activeFile      *data.DataFile            // 当前活跃数据文件
	oldFiles        map[uint32]*data.DataFile // 旧的数据文件
	index           index.Indexer             // 内存索引
	seqNo           uint64                    // 事务序列号
	isMerging       bool                      // 是否正在 merge
	seqNoFileExists bool                      // 是否支持事务
	isInitial       bool                      // 是否第一次初始化该目录
	fileLock        *flock.Flock              // 文件锁
	bytesWrite      uint                      // 累计写入且未持久化数据的大小
	reclaimSize     int64                     // 可回收数据的大小
}

// 存储引擎统计信息
type Stat struct {
	KeyNum          int   // key 数量
	DataFileNum     int   // 数据文件数量
	ReclaimableSize int64 // 可回收数据的大小
	DiskSize        int64 // 占用磁盘空间的大小
}

// 打开存储引擎实例
func Open(opts Options) (*DB, error) {
	if err := checkOptions(opts); err != nil {
		return nil, err
	}

	// 数据目录不存在则新建数据目录
	var isInitial bool
	if _, err := os.Stat(opts.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(opts.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 同一目录只能运行一个存储引擎实例
	fileLock := flock.New(filepath.Join(opts.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(opts.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	db := &DB{
		options:   opts,
		mu:        new(sync.RWMutex),
		oldFiles:  make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(opts.IndexType, opts.DirPath, opts.SyncWrites),
		isInitial: isInitial,
		fileLock:  fileLock,
	}

	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// b+树索引存储在磁盘上，不需要加载到内存
	if opts.IndexType != index.BPTREE {
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		if err := db.loadIndex(); err != nil {
			return nil, err
		}

		if db.options.MMapAtStartup {
			db.resetIOType()
		}
	}

	// 使用b+树做索引时需要加载事务号，因为不会遍历数据文件
	if opts.IndexType == index.BPTREE {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
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
		Key:   encodeKeyWithSeq(key, nonTxnSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 写入磁盘数据文件
	pos, err := db.appendLogRecordWithLock(log_record)
	if err != nil {
		return err
	}

	// 更新内存索引信息
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// 删除 key 对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// key 不存在则直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 在数据文件中写入一个墓碑值
	log_record := &data.LogRecord{
		Key:  encodeKeyWithSeq(key, nonTxnSeqNo),
		Type: data.LogRecordDeleted,
	}
	pos, err := db.appendLogRecordWithLock(log_record)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	// 删除内存索引信息
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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

	return db.getValueByPosition(pos)
}

// 获取所有的 key
func (db *DB) ListKeys() [][]byte {
	it := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	idx := 0
	for it.Rewind(); it.Valid(); it.Next() {
		keys[idx] = it.Key()
		idx++
	}
	return keys
}

// 遍历所有数据并进行指定操作，函数返回 false 时停止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	it := db.index.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		val, err := db.getValueByPosition(it.Value())
		if err != nil {
			return err
		}

		if !fn(it.Key(), val) {
			break
		}
	}

	return nil
}

// 关闭数据库
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
		Type:  data.LogRecordNormal,
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	if err := db.activeFile.Close(); err != nil {
		return err
	}
	for _, file := range db.oldFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// 获取存储引擎统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dataFiles := uint(len(db.oldFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dirsize, %v", err))
	}

	return &Stat{
		KeyNum:          db.index.Size(),
		DataFileNum:     int(dataFiles),
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// 备份数据库，将数据文件拷贝到新目录
func (db *DB) Backup(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

// 根据数据位置信息读取 value 值
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
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

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 将数据记录写入到当前活跃文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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
	db.bytesWrite += uint(size)
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: offset,
		Size:   uint32(size),
	}
	return pos, nil
}

// 设置当前活跃文件
func (db *DB) setActiveFile() error {
	var fileId uint32
	if db.activeFile != nil {
		fileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, fileId, fio.StandardIO)
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
	if opts.DataFileMergeRatio < 0 || opts.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio")
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
			splitNames := strings.Split(dirEntry.Name(), ".")
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
		ioType := fio.StandardIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId), ioType)
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

	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	// 暂存事务数据，只有读到 txnFinKey 才更新索引
	txnRecords := make(map[uint64][]*data.TransactionRecord)
	maxSeqNo := nonTxnSeqNo

	// 遍历所有数据文件，并把记录加载到索引
	for _, fid := range db.fileIds {
		fileId := uint32(fid)

		// 说明已经 merge 过，不需要加载索引
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.oldFiles[fileId]
		}

		// 循环读取数据文件中记录
		var offset int64
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			pos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
				Size:   uint32(size),
			}

			key, seqNo := decodeKeyWithSeq(logRecord.Key)
			if seqNo > maxSeqNo {
				maxSeqNo = seqNo
			}
			if seqNo == nonTxnSeqNo {
				db.updateIndex(key, pos, logRecord.Type)
			} else {
				switch logRecord.Type {
				case data.LogRecordNormal:
					{
						txnRecords[seqNo] = append(txnRecords[seqNo], &data.TransactionRecord{
							Key:  key,
							Pos:  pos,
							Type: data.LogRecordNormal,
						})
					}
				case data.LogRecordDeleted:
					{
						txnRecords[seqNo] = append(txnRecords[seqNo], &data.TransactionRecord{
							Key:  key,
							Pos:  pos,
							Type: data.LogRecordDeleted,
						})
					}
				case data.LogRecordTxnFinished:
					{
						for _, txnRecord := range txnRecords[seqNo] {
							if err := db.updateIndex(txnRecord.Key, txnRecord.Pos, txnRecord.Type); err != nil {
								return err
							}
						}
					}
				}

			}

			offset += size
		}

		// 如果是活跃文件，更新写入偏移
		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOff = offset
		}
	}

	db.seqNo = maxSeqNo + 1

	return nil
}

// 更新索引
func (db *DB) updateIndex(key []byte, pos *data.LogRecordPos, typ data.LogRecordType) error {
	var oldPos *data.LogRecordPos
	if typ == data.LogRecordNormal {
		oldPos = db.index.Put(key, pos)
	}
	if typ == data.LogRecordDeleted {
		op, ok := db.index.Delete(key)
		if !ok {
			return ErrIndexUpdateFailed
		}
		db.reclaimSize += int64(pos.Size)
		oldPos = op
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName)
}

// 将文件 IO 类型重置为标准文件 IO
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardIO); err != nil {
		return err
	}
	for _, dataFile := range db.oldFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardIO); err != nil {
			return err
		}
	}
	return nil
}
