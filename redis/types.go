package redis

import (
	bitcask "bitcask-go"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("wrong type operation")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// Redis 数据结构服务
type RedisDataStructure struct {
	db *bitcask.DB
}

func NewRedisDataStructure(opts bitcask.Options) (*RedisDataStructure, error) {
	db, err := bitcask.Open(opts)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

// ======================= String 数据结构 =======================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 value : 类型 + 过期时间 + 实际数据
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	index := 1
	var expire int64
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	datatype := encValue[0]
	if datatype != String {
		return nil, ErrWrongTypeOperation
	}
	index := 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	// 判断 key 是否过期
	if expire > 0 && time.Now().UnixNano() >= expire {
		return nil, nil
	}

	return encValue[index:], nil
}
