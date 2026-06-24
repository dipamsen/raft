package raft

import (
	"fmt"
	"sync"
)

type MockNetwork struct {
	mu    sync.RWMutex
	nodes map[uint64]*Raft
}

// Creates a new in-memory mock network for Raft nodes.
func NewMockNetwork() *MockNetwork {
	return &MockNetwork{
		nodes: make(map[uint64]*Raft),
	}
}

// Adds a node to the mock network so it can receive RPCs.
func (m *MockNetwork) Register(node *Raft) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.id] = node
}

// Removes a node from the mock network.
func (m *MockNetwork) Unregister(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, id)
}

// Routes an RPC invocation to the target node using the mock network.
func (m *MockNetwork) Call(id uint64, rpcName string, args any, reply any) error {
	m.mu.RLock()
	target, ok := m.nodes[id]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("mock network: node %d not registered", id)
	}

	switch rpcName {
	case "Raft.AppendEntries":
		req, ok := args.(AppendEntriesArgs)
		if !ok {
			return fmt.Errorf("mock network: expected AppendEntriesArgs, got %T", args)
		}
		respCh := make(chan AppendEntriesResponse, 1)
		target.appendEntriesCh <- AppendEntriesEvent{args: req, replyCh: respCh}
		resp := <-respCh
		out, ok := reply.(*AppendEntriesResponse)
		if !ok {
			return fmt.Errorf("mock network: expected *AppendEntriesResponse reply, got %T", reply)
		}
		*out = resp
		return nil

	case "Raft.RequestVote":
		req, ok := args.(RequestVoteArgs)
		if !ok {
			return fmt.Errorf("mock network: expected RequestVoteArgs, got %T", args)
		}
		respCh := make(chan RequestVoteResponse, 1)
		target.requestVoteCh <- RequestVoteEvent{args: req, replyCh: respCh}
		resp := <-respCh
		out, ok := reply.(*RequestVoteResponse)
		if !ok {
			return fmt.Errorf("mock network: expected *RequestVoteResponse reply, got %T", reply)
		}
		*out = resp
		return nil

	case "Raft.ClientCommand":
		req, ok := args.(ClientCommandArgs)
		if !ok {
			return fmt.Errorf("mock network: expected ClientCommandArgs, got %T", args)
		}
		respCh := make(chan ClientCommandResponse, 1)
		target.clientCommandCh <- ClientCommandEvent{args: req, replyCh: respCh}
		resp := <-respCh
		out, ok := reply.(*ClientCommandResponse)
		if !ok {
			return fmt.Errorf("mock network: expected *ClientCommandResponse reply, got %T", reply)
		}
		*out = resp
		return nil

	default:
		return fmt.Errorf("mock network: unknown rpc %s", rpcName)
	}
}
