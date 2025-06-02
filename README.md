# Bitcask-based KV Storage Engine with Redis Protocol Support

## 项目概述
一个基于 **Bitcask 存储模型** 的高性能键值存储引擎，兼容 **Redis 协议**。

## 快速开始

```bash
# 启动服务端
cd redis/cmd/
go build -o bitcask-redis  
./bitcask-redis            

# 使用 redis-cli 客户端连接
redis-cli -p 6379 
