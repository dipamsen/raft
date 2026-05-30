package main

import (
	"fmt"
	"time"

	"github.com/dipamsen/raft"
)

func main() {
	fmt.Println("Hello, Raft!")

	network := raft.NewMockNetwork()

	node1 := raft.NewRaft(1, []uint64{2, 3})
	node2 := raft.NewRaft(2, []uint64{1, 3})
	node3 := raft.NewRaft(3, []uint64{1, 2})

	node1.SetNetwork(network)
	node2.SetNetwork(network)
	node3.SetNetwork(network)

	network.Register(node1)
	network.Register(node2)
	network.Register(node3)

	go node1.Run()
	go node2.Run()
	go node3.Run()

	time.Sleep(2 * time.Second)
	if err := node1.ClientCommand(raft.Command{Key: "foo", Value: 42}); err != nil {
		fmt.Println("client command failed:", err)
	} else {
		fmt.Println("client command submitted")
	}

	time.Sleep(3 * time.Second)
}
