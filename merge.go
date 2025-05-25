package bitcask_go

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	mergeDirName = "-merge"
	mergeFinKey  = "merge-fin"
)

// 清理无效数据，并生成 Hint 文件
func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	// 同时只允许一个线程进行 merge
	db.mu.Lock()
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeInProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.oldFiles[db.activeFile.FileId] = db.activeFile
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 记录 merge 临界点的文件 id
	nonMergeFileId := db.activeFile.FileId

	// 获取快照，包括所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.oldFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 根据文件 id 从小到大依次 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	// 新建 merge 目录，如果已存在则先删除
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 在 merge 目录下打开新的存储引擎实例
	mergeOpts := db.options
	mergeOpts.DirPath = mergePath
	mergeOpts.SyncWrites = false
	mergeDB, err := Open(mergeOpts)
	if err != nil {
		return err
	}

	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 遍历所有数据文件，如果数据有效则重写数据
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 解析拿到实际的 key，重写时不再需要事务标记
			key, _ := decodeKeyWithSeq(logRecord.Key)

			// 和内存索引位置进行比较，如果相同说明数据有效
			pos := db.index.Get(key)
			if pos != nil && pos.Fid == dataFile.FileId && pos.Offset == offset {
				logRecord.Key = key
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 将位置索引写入到 Hint 文件中
				if err := hintFile.WriteHintRecord(key, pos); err != nil {
					return err
				}
			}

			offset += size
		}
	}

	// 持久化数据
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 启动时读取标识 merge 完成的文件，其中记录那些文件已完成 merge
	mergeFinFile, err := data.OpenMergeFinFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
		Type:  data.LogRecordNormal,
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 加载 merge 目录
func (db *DB) loadMergeFiles() error {
	// 不存在则直接返回
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		os.RemoveAll(mergePath)
	}()

	entries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识 merge 完成的文件
	mergeFinished := false
	var mergeFileNames []string
	for _, entry := range entries {
		if entry.Name() == data.MergeFinFileName {
			mergeFinished = true
		}
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			mergeFileNames = append(mergeFileNames, entry.Name())
		}
	}
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的数据文件
	var fileId uint32
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(mergePath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dscPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dscPath); err != nil {
			return err
		}
	}

	return nil
}

// 拿到最近没有参与 merge 的文件 id
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinFile, err := data.OpenMergeFinFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	id, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); err != nil {
		return nil
	}

	hintFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 将位置信息加载到索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)

		offset += size
	}

	return nil
}
