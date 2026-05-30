# raft

_A basic Raft implementation in Go._

Raft is a *consensus algorithm* for a replicated state machine. It is designed to be easy to understand and implement, and to be performant enough for use in production systems.

Consensus is a fundamental problem in distributed systems, where multiple nodes need to agree on a single state. Raft achieves consensus by electing a leader node that manages the replicated state machine. The leader is responsible for handling client requests and replicating the state machine across the cluster. If a leader fails (which can be detected by a heartbeat mechanism), a new leader is elected and the old leader steps down.

This implementation is based on the extensive description of the Raft algorithm in the extended Raft paper.

- [Raft paper](https://raft.github.io/raft.pdf)
- [Raft website](https://raft.github.io/)

## Demo

Run the demo with:

```
go run cmd/demo/main.go
```

## License

[MIT](LICENSE)