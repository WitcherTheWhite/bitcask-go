package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"bitcask-go/utils"
	"errors"
	"strings"

	"github.com/tidwall/redcon"
)

var errInvalidArgs = errors.New("invalid arguments")

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var commands = map[string]cmdHandler{
	"quit":      nil,
	"ping":      nil,
	"set":       set,
	"get":       get,
	"hset":      hset,
	"hget":      hget,
	"hdel":      hdel,
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,
	"lpush":     lpush,
	"rpush":     rpush,
	"lpop":      lpop,
	"rpop":      rpop,
	"zadd":      zadd,
	"zscore":    zscore,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcask_redis.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, exist := commands[command]
	if !exist {
		conn.WriteError("unsupported command: " + command)
		return
	}

	cli, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit":
		conn.Close()
	case "ping":
		conn.WriteString("pong")
	default:
		res, err := cmdFunc(cli, cmd.Args[1:])
		if err != nil {
			if err == bitcask.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("ok"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errInvalidArgs
	}
	key := args[0]
	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errInvalidArgs
	}
	key, field, value := args[0], args[1], args[2]
	ok, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return redcon.SimpleString("ok"), nil
}

func hget(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, field := args[0], args[1]
	value, err := cli.db.HGet(key, field)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hdel(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, field := args[0], args[1]
	ok, err := cli.db.HDel(key, field)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return redcon.SimpleString("ok"), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, member := args[0], args[1]
	ok, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return redcon.SimpleString("ok"), nil
}

func sismember(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, member := args[0], args[1]
	ok, err := cli.db.SIsMember(key, member)
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleString("yes"), nil
	} else {
		return redcon.SimpleString("no"), nil
	}
}

func srem(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, member := args[0], args[1]
	ok, err := cli.db.SRem(key, member)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return redcon.SimpleString("ok"), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, element := args[0], args[1]
	size, err := cli.db.LPush(key, element)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(size), nil
}

func rpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, element := args[0], args[1]
	size, err := cli.db.RPush(key, element)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(size), nil
}

func lpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errInvalidArgs
	}
	key := args[0]
	element, err := cli.db.LPop(key)
	if err != nil {
		return nil, err
	}
	return element, nil
}

func rpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errInvalidArgs
	}
	key := args[0]
	element, err := cli.db.RPop(key)
	if err != nil {
		return nil, err
	}
	return element, nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errInvalidArgs
	}
	key, score, member := args[0], args[1], args[2]
	ok, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return redcon.SimpleString("ok"), nil
}

func zscore(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errInvalidArgs
	}
	key, member := args[0], args[1]
	score, err := cli.db.ZScore(key, member)
	if err != nil {
		return nil, err
	}
	return score, nil
}
