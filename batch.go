package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTxnSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// 批量写数据
type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
	options       WriteBatchOptions
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: map[string]*data.LogRecord{},
		options:       opts,
	}
}

// 暂存写入的数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	lr := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	wb.pendingWrites[string(key)] = lr
	return nil
}

// 暂存删除的数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	pos := wb.db.index.Get(key)
	if pos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	lr := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = lr
	return nil
}

// 将暂存数据写入到数据文件，更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchSize {
		return ErrExceedMaxBatchSize
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前事务的序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 暂存数据全部写入磁盘
	postions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   encodeKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		postions[string(record.Key)] = pos
	}

	// 写一条标识事务完成的数据
	finRecord := &data.LogRecord{
		Key:  encodeKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	_, err := wb.db.appendLogRecord(finRecord)
	if err != nil {
		return err
	}

	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := postions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// 编码包括 key 和事务序列号
func encodeKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	b := make([]byte, n+len(key))
	copy(b[:n], seq[:n])
	copy(b[n:], key)
	return b
}

// 解码得到 key 和事务序列号
func decodeKeyWithSeq(b []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(b)
	key := b[n:]
	return key, seqNo
}
