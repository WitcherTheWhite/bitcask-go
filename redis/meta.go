package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetaDataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

// 元数据
type metadata struct {
	dataType byte   // 数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号
	size     uint32 // 数据量
	head     uint64 // List 专用
	tail     uint64 // List 专用
}

func (md *metadata) encode() []byte {
	size := maxMetaDataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)

	buf[0] = md.dataType
	index := 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetadata(b []byte) *metadata {
	dataType := b[0]

	index := 1
	expire, n := binary.Varint(b[index:])
	index += n
	version, n := binary.Varint(b[index:])
	index += n
	size, n := binary.Varint(b[index:])
	index += n

	var head, tail uint64
	if dataType == List {
		head, n = binary.Uvarint(b[index:])
		index += n
		tail, _ = binary.Uvarint(b[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+8+len(hk.field))

	// key
	index := 0
	copy(buf[:len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// field
	copy(buf[index:index+len(hk.field)], hk.field)
	index += len(hk.field)

	return buf
}
