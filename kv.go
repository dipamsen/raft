package raft

import "sync"

type KV struct {
	mu sync.RWMutex

	data map[string]uint64
}

func NewKV() *KV {
	return &KV{
		data: make(map[string]uint64),
	}
}

func (k *KV) Set(key string, value uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.data[key] = value
}

func (k *KV) Get(key string) uint64 {
	k.mu.RLock()
	defer k.mu.RUnlock()

	return k.data[key]
}

func (k *KV) Apply(cmd Command) {
	k.Set(cmd.Key, cmd.Value)
}
