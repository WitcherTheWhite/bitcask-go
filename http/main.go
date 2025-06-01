package main

import (
	bitcask "bitcask-go"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

// 初始化 db 实例
func init() {
	opts := bitcask.DefaultOptions
	dir, err := os.MkdirTemp("", "bitcask-go-http")
	if err != nil {
		panic(fmt.Sprintf("failed to make directory, %v", err))
	}
	opts.DirPath = dir
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("failed to open db, %v", err))
	}
}

func handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "bad requese", http.StatusBadRequest)
		return
	}

	for k, v := range data {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value: %v\n", err)
			return
		}
	}
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	val, err := db.Get([]byte(key))
	if err != nil && err != bitcask.ErrKeyNotFound {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value: %v\n", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(string(val))
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && err != bitcask.ErrKeyNotFound {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete value: %v\n", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode("ok")
}

func handleListKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	var res []string
	for _, key := range keys {
		res = append(res, string(key))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func handleStat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stat)
}

func main() {
	// 注册处理方法
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)

	// 启动 HTTP 服务
	_ = http.ListenAndServe("localhost:8080", nil)
}
