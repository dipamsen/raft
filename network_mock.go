package raft

import (
	"fmt"
	"sync"
)

// A simple pair struct to represent a directed link from -> to
type link struct {
	from uint64
	to   uint64
}

type MockNetwork struct {
	mu            sync.RWMutex
	nodes         map[uint64]*Raft
	disabledLinks map[link]bool // Tracks blocked communication paths
}

// Creates a new in-memory mock network for Raft nodes.
func NewMockNetwork() *MockNetwork {
	return &MockNetwork{
		nodes:         make(map[uint64]*Raft),
		disabledLinks: make(map[link]bool),
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

	for l := range m.disabledLinks {
		if l.from == id || l.to == id {
			delete(m.disabledLinks, l)
		}
	}
}

// Isolates a specific set of nodes from the rest of the network,
func (m *MockNetwork) Partition(subsets ...[]uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.disabledLinks = make(map[link]bool)

	getSubsetIdx := func(id uint64) int {
		for i, subset := range subsets {
			for _, nodeID := range subset {
				if nodeID == id {
					return i
				}
			}
		}
		return -1
	}

	for fromID := range m.nodes {
		for toID := range m.nodes {
			if fromID == toID {
				continue
			}
			fromIdx := getSubsetIdx(fromID)
			toIdx := getSubsetIdx(toID)

			if fromIdx == -1 || toIdx == -1 || fromIdx != toIdx {
				m.disabledLinks[link{from: fromID, to: toID}] = true
			}
		}
	}
}

// Heals all partitions, restoring full connectivity.
func (m *MockNetwork) Heal() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disabledLinks = make(map[link]bool)
}

// Routes an RPC invocation to the target node using the mock network.
// It will fail if a network partition blocks the sender from reaching the target.
func (m *MockNetwork) Call(fromID uint64, id uint64, rpcName string, args any, reply any) error {
	m.mu.RLock()
	if m.disabledLinks[link{from: fromID, to: id}] {
		m.mu.RUnlock()
		return fmt.Errorf("mock network: drop RPC %s due to network partition between %d and %d", rpcName, fromID, id)
	}

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
