package raft

import "sync"

type KV struct {
	mu sync.RWMutex

	data map[string]uint64
}

// Creates a new in-memory key/value state machine.
func NewKV() *KV {
	return &KV{
		data: make(map[string]uint64),
	}
}

// Stores a value for the given key in the state machine.
func (k *KV) Set(key string, value uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.data[key] = value
}

// Returns the current value for a key from the state machine.
func (k *KV) Get(key string) uint64 {
	k.mu.RLock()
	defer k.mu.RUnlock()

	return k.data[key]
}

// Applies a Raft command to the state machine.
func (k *KV) Apply(cmd Command) {
	k.Set(cmd.Key, cmd.Value)
}
