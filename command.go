package raft

type Command struct {
	Key   string
	Value uint64
}
