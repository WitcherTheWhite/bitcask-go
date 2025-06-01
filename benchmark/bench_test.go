package benchmark

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var db *bitcask.DB

// 初始化 db 实例
func init() {
	opts := bitcask.DefaultOptions
	dir, err := os.MkdirTemp("", "bitcask-go-bench")
	if err != nil {
		panic(fmt.Sprintf("failed to make directory, %v", err))
	}
	opts.DirPath = dir
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("failed to open db, %v", err))
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(r.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(r.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
