package raft

type Network interface {
	Call(fromID uint64, id uint64, rpcName string, args interface{}, reply interface{}) error
}
