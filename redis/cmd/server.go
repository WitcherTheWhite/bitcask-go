package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6379"

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     *sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	rds, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs:    map[int]*bitcask_redis.RedisDataStructure{},
		server: &redcon.Server{},
		mu:     &sync.RWMutex{},
	}
	bitcaskServer.dbs[0] = rds

	// 初始化一个 Redis 服务端
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.closed)
	bitcaskServer.listen()
}

func (bs *BitcaskServer) listen() {
	log.Println("bitcask server running...")
	_ = bs.server.ListenAndServe()
}

func (bs *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	bs.mu.Lock()
	defer bs.mu.Unlock()
	cli.server = bs
	cli.db = bs.dbs[0]
	conn.SetContext(cli)
	return true
}

func (bs *BitcaskServer) closed(conn redcon.Conn, err error) {
	for _, db := range bs.dbs {
		db.Close()
	}
	bs.server.Close()
}
