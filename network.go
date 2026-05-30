package raft

type Network interface {
	Call(id uint64, rpcName string, args interface{}, reply interface{}) error
}
