package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty         = errors.New("key is emty")
	ErrIndexUpdateFailed  = errors.New("failed to update index")
	ErrKeyNotFound        = errors.New("key not found in database")
	ErrDataFileNotFound   = errors.New("datafile not found in database")
	ErrDataFileCorrupted  = errors.New("datafile may corrupted")
	ErrExceedMaxBatchSize = errors.New("exceed max batchsize")
	ErrMergeInProgress    = errors.New("database is merging")
	ErrDatabaseIsUsing    = errors.New("database is using")
)
