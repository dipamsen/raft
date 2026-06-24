package main

import (
	"fmt"
	"time"

	"github.com/dipamsen/raft"
)

const numNodes = 5

func main() {
	net := raft.NewMockNetwork()

	ids := []uint64{1, 2, 3, 4, 5} // NOTE: id 0 is reserved as a "no leader / not voted" sentinel in this impl - never use it.
	nodes := make(map[uint64]*raft.Raft, numNodes)

	for _, id := range ids {
		peers := peersOf(id, ids)
		node := raft.NewRaft(id, peers)
		node.SetNetwork(net)
		net.Register(node)
		nodes[id] = node
		go node.Run()
	}

	fmt.Println("5-node Raft cluster started")
	waitForLeader(nodes, 2*time.Second)
	printCluster(nodes)

	leader := findLeader(nodes)
	if leader == nil {
		fmt.Println("no leader elected, aborting demo")
		return
	}

	fmt.Printf("\nSubmitting client commands:\n")
	commands := []raft.Command{
		{Key: "x", Value: 1},
		{Key: "y", Value: 2},
		{Key: "x", Value: 42},
	}
	for _, cmd := range commands {
		if err := leader.ClientCommand(cmd); err != nil {
			fmt.Printf("command %+v failed: %v\n", cmd, err)
			continue
		}
		fmt.Printf("submitted: %s = %d\n", cmd.Key, cmd.Value)
	}

	time.Sleep(300 * time.Millisecond)
	fmt.Println("\nAfter replication:")
	printCluster(nodes)
	printKV(nodes, "x")
	printKV(nodes, "y")

	crashedID := leader.ID()
	fmt.Printf("\nCrashing leader %d...\n", crashedID)
	leader.Stop()
	net.Unregister(crashedID)
	delete(nodes, crashedID)

	fmt.Println("waiting for the remaining 4 nodes to elect a new leader...")
	waitForLeader(nodes, 3*time.Second)
	printCluster(nodes)

	println("")
	newLeader := findLeader(nodes)
	if newLeader == nil {
		fmt.Println("no new leader elected after crash - cluster unavailable")
		return
	}
	if newLeader.ID() == crashedID {
		fmt.Println("unexpected: crashed node still reported as leader")
		return
	}
	fmt.Printf("new leader elected: node %d (old leader %d is down)\n", newLeader.ID(), crashedID)

	println("")
	cmd := raft.Command{Key: "z", Value: 100}
	if err := newLeader.ClientCommand(cmd); err != nil {
		fmt.Printf("command failed: %v\n", err)
	} else {
		fmt.Printf("submitted: %s = %d\n", cmd.Key, cmd.Value)
	}

	time.Sleep(300 * time.Millisecond)
	println("")
	printCluster(nodes)
	printKV(nodes, "x")
	printKV(nodes, "y")
	printKV(nodes, "z")
}

func peersOf(self uint64, all []uint64) []uint64 {
	peers := make([]uint64, 0, len(all)-1)
	for _, id := range all {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

func waitForLeader(nodes map[uint64]*raft.Raft, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if findLeader(nodes) != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func findLeader(nodes map[uint64]*raft.Raft) *raft.Raft {
	for _, n := range nodes {
		if n.GetStatus().Role == raft.Leader {
			return n
		}
	}
	return nil
}

func printCluster(nodes map[uint64]*raft.Raft) {
	ids := make([]uint64, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	for _, id := range ids {
		s := nodes[id].GetStatus()
		fmt.Printf("  node %d: role=%-9s term=%d leader=%d logLen=%d commitIndex=%d lastApplied=%d\n",
			s.ID, s.Role, s.CurrentTerm, s.LeaderId, s.LogLen, s.CommitIndex, s.LastApplied)
	}
}

func printKV(nodes map[uint64]*raft.Raft, key string) {
	fmt.Printf("  %s -> ", key)
	first := true
	for _, id := range sortedIDs(nodes) {
		if !first {
			fmt.Print(", ")
		}
		first = false
		fmt.Printf("node%d=%d", id, nodes[id].Get(key))
	}
	fmt.Println()
}

func sortedIDs(nodes map[uint64]*raft.Raft) []uint64 {
	ids := make([]uint64, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	return ids
}
