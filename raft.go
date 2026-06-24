package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	Term uint64
	Data Command
}

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

type Raft struct {
	mu sync.Mutex

	id    uint64
	peers []uint64
	role  Role
	state KV
	net   Network

	leaderId uint64

	// latest term server has seen
	currentTerm uint64
	// candidateId that received vote in current term (or null if none)
	votedFor uint64
	// log entries; index 0 is a dummy sentinel entry with Term=0
	log []LogEntry

	// index of highest log entry known to be committed
	commitIndex uint64
	// index of highest log entry applied to state machine
	lastApplied uint64

	// LEADER STATE
	// for each server, index of the next log entry to send to that server
	nextIndex map[uint64]uint64
	// for each server, index of highest log entry known to be replicated on server
	matchIndex map[uint64]uint64

	electionTimer *time.Timer

	appendEntriesCh chan AppendEntriesEvent
	requestVoteCh   chan RequestVoteEvent
	clientCommandCh chan ClientCommandEvent
	voteResult      chan VoteResult
	stopCh          chan struct{}
	stopped         bool
}

// Creates a new Raft node with the given id and peer ids.
// It initializes internal state, timers, and channels.
func NewRaft(id uint64, peers []uint64) *Raft {
	r := &Raft{
		id:              id,
		peers:           peers,
		role:            Follower,
		currentTerm:     0,
		votedFor:        0,
		log:             []LogEntry{{Term: 0}},
		commitIndex:     0,
		lastApplied:     0,
		state:           KV{data: make(map[string]uint64)},
		nextIndex:       make(map[uint64]uint64),
		matchIndex:      make(map[uint64]uint64),
		appendEntriesCh: make(chan AppendEntriesEvent, 100),
		requestVoteCh:   make(chan RequestVoteEvent, 100),
		clientCommandCh: make(chan ClientCommandEvent, 100),
		voteResult:      make(chan VoteResult, len(peers)),
		stopCh:          make(chan struct{}),
	}
	r.electionTimer = time.NewTimer(r.randomElectionTimeout())
	return r
}

// Assigns the network implementation used to route RPCs for this Raft node.
func (r *Raft) SetNetwork(net Network) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.net = net
}

// Submits a client command to this Raft node, forwarding it to the leader if this node is not the leader.
func (r *Raft) ClientCommand(cmd Command) error {
	r.mu.Lock()
	role := r.role
	leaderId := r.leaderId
	term := r.currentTerm
	r.mu.Unlock()

	if role == Leader {
		r.mu.Lock()
		r.log = append(r.log, LogEntry{Term: term, Data: cmd})
		r.mu.Unlock()
		go r.broadcastHeartbeat()
		return nil
	}

	if leaderId == 0 {
		return fmt.Errorf("no leader known")
	}
	if r.net == nil {
		return fmt.Errorf("no network configured")
	}

	resp := ClientCommandResponse{}
	err := r.net.Call(r.id, leaderId, "Raft.ClientCommand", ClientCommandArgs{Command: cmd}, &resp)
	if err != nil {
		return err
	}
	if !resp.Success {
		if resp.LeaderId != 0 && resp.LeaderId != leaderId {
			return fmt.Errorf("leader redirect to %d: %s", resp.LeaderId, resp.Error)
		}
		return fmt.Errorf("client command rejected: %s", resp.Error)
	}
	return nil
}

// Returns a randomized election timeout for leader election timeouts.
func (r *Raft) randomElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(150+rand.Intn(150))
}

// Signals the node to shut down. It is safe to call multiple times
// and from any goroutine.
func (r *Raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	r.stopped = true
	close(r.stopCh)
}

// Executes the Raft node's main event loop.
func (r *Raft) Run() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		r.mu.Lock()
		role := r.role
		r.mu.Unlock()

		switch role {
		case Follower:
			r.RunFollower()
		case Candidate:
			r.RunCandidate()
		case Leader:
			r.RunLeader()
		}

		r.applyCommitted()
	}
}

// Spplies any committed but not yet applied log entries to the local state machine.
func (r *Raft) applyCommitted() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyCommittedLocked()
}

// Applies any newly-committed log entries to the state
// machine. Caller must already hold r.mu.
func (r *Raft) applyCommittedLocked() {
	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		r.state.Apply(r.log[r.lastApplied].Data)
	}
}

// Runs the follower event loop, handling RPCs and election timeouts.
func (r *Raft) RunFollower() {
	for {
		select {
		case <-r.stopCh:
			return

		case <-r.electionTimer.C:
			r.mu.Lock()
			r.role = Candidate
			r.mu.Unlock()
			r.startElection()
			return

		case event := <-r.appendEntriesCh:
			r.mu.Lock()
			args := event.args
			var resp AppendEntriesResponse
			if args.term < r.currentTerm {
				resp = AppendEntriesResponse{term: r.currentTerm, success: false}
			} else {
				// uniformly update term, leader, and reset timer for any
				// valid (term >= currentTerm) AppendEntries
				if args.term > r.currentTerm {
					r.currentTerm = args.term
					r.votedFor = 0
				}
				r.leaderId = args.leaderId
				r.resetTimer()
				resp = r.handleAppendEntries(args)
			}
			r.mu.Unlock()
			event.replyCh <- resp

		case event := <-r.requestVoteCh:
			r.mu.Lock()
			args := event.args
			if args.term > r.currentTerm {
				r.currentTerm = args.term
				r.votedFor = 0
			}
			resp := r.handleRequestVote(args)
			r.mu.Unlock()
			event.replyCh <- resp

		case event := <-r.clientCommandCh:
			r.mu.Lock()
			leaderId := r.leaderId
			r.mu.Unlock()
			resp := ClientCommandResponse{Success: false, LeaderId: leaderId, Error: "not leader"}
			if leaderId == 0 {
				resp.Error = "leader unknown"
			}
			event.replyCh <- resp
		}
	}
}

// Runs the candidate event loop, handling election retries and incoming RPCs.
func (r *Raft) RunCandidate() {
	for {
		select {
		case <-r.stopCh:
			return

		case <-r.electionTimer.C:
			r.startElection()

		case event := <-r.appendEntriesCh:
			r.mu.Lock()
			args := event.args
			if args.term >= r.currentTerm {
				r.currentTerm = args.term
				r.votedFor = 0
				r.leaderId = args.leaderId
				r.role = Follower
				resp := r.handleAppendEntries(args)
				r.mu.Unlock()
				event.replyCh <- resp
				return
			}
			resp := AppendEntriesResponse{term: r.currentTerm, success: false}
			r.mu.Unlock()
			event.replyCh <- resp

		case event := <-r.requestVoteCh:
			r.mu.Lock()
			args := event.args
			if args.term > r.currentTerm {
				r.currentTerm = args.term
				r.votedFor = 0
				r.role = Follower
				resp := r.handleRequestVote(args)
				r.mu.Unlock()
				event.replyCh <- resp
				return
			}
			resp := RequestVoteResponse{term: r.currentTerm, voteGranted: false}
			r.mu.Unlock()
			event.replyCh <- resp

		case event := <-r.clientCommandCh:
			r.mu.Lock()
			leaderId := r.leaderId
			r.mu.Unlock()
			resp := ClientCommandResponse{Success: false, LeaderId: leaderId, Error: "not leader"}
			if leaderId == 0 {
				resp.Error = "leader unknown"
			}
			event.replyCh <- resp

		case result := <-r.voteResult:
			r.mu.Lock()
			if result.granted && result.term == r.currentTerm && r.role == Candidate {
				r.role = Leader
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
	}
}

// Runs the leader event loop, sending heartbeats and replicating client commands.
func (r *Raft) RunLeader() {
	r.mu.Lock()
	for _, p := range r.peers {
		r.nextIndex[p] = uint64(len(r.log))
		r.matchIndex[p] = 0
	}
	r.mu.Unlock()

	r.broadcastHeartbeat()

	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return

		case <-ticker.C:
			r.broadcastHeartbeat()

		case event := <-r.appendEntriesCh:
			r.mu.Lock()
			args := event.args
			if args.term >= r.currentTerm {
				r.currentTerm = args.term
				r.votedFor = 0
				r.leaderId = args.leaderId
				r.role = Follower
				resp := r.handleAppendEntries(args)
				r.mu.Unlock()
				event.replyCh <- resp
				return
			}
			resp := AppendEntriesResponse{term: r.currentTerm, success: false}
			r.mu.Unlock()
			event.replyCh <- resp

		case event := <-r.requestVoteCh:
			r.mu.Lock()
			args := event.args
			if args.term > r.currentTerm {
				r.currentTerm = args.term
				r.votedFor = 0
				r.role = Follower
				resp := r.handleRequestVote(args)
				r.mu.Unlock()
				event.replyCh <- resp
				return
			}
			resp := RequestVoteResponse{term: r.currentTerm, voteGranted: false}
			r.mu.Unlock()
			event.replyCh <- resp

		case event := <-r.clientCommandCh:
			r.mu.Lock()
			entry := LogEntry{Term: r.currentTerm, Data: event.args.Command}
			r.log = append(r.log, entry)
			r.mu.Unlock()
			event.replyCh <- ClientCommandResponse{Success: true, LeaderId: r.id}
			go r.broadcastHeartbeat()
		}
	}
}

// Begins a new election by incrementing the term, voting for self, and requesting votes from peers.
func (r *Raft) startElection() {
	r.mu.Lock()
	r.currentTerm++
	r.votedFor = r.id
	r.resetTimer()
	term := r.currentTerm
	lastLogIndex := uint64(len(r.log) - 1)
	args := RequestVoteArgs{
		term:         term,
		candidateId:  r.id,
		lastLogIndex: lastLogIndex,
		lastLogTerm:  r.log[lastLogIndex].Term,
	}
	peers := make([]uint64, len(r.peers))
	copy(peers, r.peers)
	r.mu.Unlock()

	votesCh := make(chan VoteResult, len(peers))
	for _, peer := range peers {
		go func(peer uint64) {
			reply := r.sendRequestVote(peer, args)
			if reply.term > term {
				r.mu.Lock()
				if reply.term > r.currentTerm {
					r.currentTerm = reply.term
					r.votedFor = 0
					r.role = Follower
				}
				r.mu.Unlock()
			}
			votesCh <- VoteResult{term: reply.term, granted: reply.voteGranted && reply.term == term}
		}(peer)
	}

	total := len(peers) + 1
	go func() {
		votes := 1
		for range peers {
			result := <-votesCh
			if result.granted {
				votes++
			}
			if votes*2 > total {
				r.voteResult <- VoteResult{term: term, granted: true}
				return
			}
		}
	}()
}

// Sends AppendEntries RPCs to all peers and processes replies.
func (r *Raft) broadcastHeartbeat() {
	r.mu.Lock()
	type peerArgs struct {
		peer uint64
		args AppendEntriesArgs
	}
	sends := make([]peerArgs, 0, len(r.peers))
	for _, peer := range r.peers {
		ni := max(r.nextIndex[peer], 1)
		prevLogIndex := ni - 1
		prevLogTerm := r.log[prevLogIndex].Term

		entries := make([]LogEntry, len(r.log[ni:]))
		copy(entries, r.log[ni:])

		sends = append(sends, peerArgs{
			peer: peer,
			args: AppendEntriesArgs{
				term:         r.currentTerm,
				leaderId:     r.id,
				prevLogIndex: prevLogIndex,
				prevLogTerm:  prevLogTerm,
				entries:      entries,
				leaderCommit: r.commitIndex,
			},
		})
	}
	r.mu.Unlock()

	for _, s := range sends {
		go func(s peerArgs) {
			reply := r.sendAppendEntries(s.peer, s.args)
			r.mu.Lock()
			defer r.mu.Unlock()
			if reply.term > r.currentTerm {
				r.currentTerm = reply.term
				r.votedFor = 0
				r.role = Follower
				return
			}
			if r.role != Leader {
				return
			}
			if reply.success {
				newMatchIndex := s.args.prevLogIndex + uint64(len(s.args.entries))
				if newMatchIndex > r.matchIndex[s.peer] {
					r.matchIndex[s.peer] = newMatchIndex
					r.nextIndex[s.peer] = newMatchIndex + 1
				}
				r.maybeAdvanceCommitIndex()
			} else {
				if r.nextIndex[s.peer] > 1 {
					r.nextIndex[s.peer]--
				}
			}
		}(s)
	}
}

// advances commitIndex to the highest index
// that is replicated on a majority. Must be called with r.mu held.
func (r *Raft) maybeAdvanceCommitIndex() {
	// Walk backwards from the end of the log to find the highest N such that:
	//   - N > commitIndex
	//   - log[N].Term == currentTerm  (only commit from current term)
	//   - a majority have matchIndex >= N
	for n := uint64(len(r.log) - 1); n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			continue
		}
		count := 1
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				count++
			}
		}
		if count*2 > len(r.peers)+1 {
			r.commitIndex = n
			r.applyCommittedLocked()
			break
		}
	}
}

// Processes an AppendEntries RPC and updates the log and commit state if the request is valid.
func (r *Raft) handleAppendEntries(args AppendEntriesArgs) AppendEntriesResponse {
	if args.term < r.currentTerm {
		return AppendEntriesResponse{term: r.currentTerm, success: false}
	}

	prevLogIndex := args.prevLogIndex
	prevLogTerm := args.prevLogTerm
	if prevLogIndex >= uint64(len(r.log)) || r.log[prevLogIndex].Term != prevLogTerm {
		return AppendEntriesResponse{term: r.currentTerm, success: false}
	}

	i := prevLogIndex + 1
	j := uint64(0)
	for j < uint64(len(args.entries)) && i < uint64(len(r.log)) {
		if r.log[i].Term != args.entries[j].Term {
			r.log = r.log[:i]
			break
		}
		i++
		j++
	}
	r.log = append(r.log, args.entries[j:]...)

	if args.leaderCommit > r.commitIndex {
		r.commitIndex = min(args.leaderCommit, uint64(len(r.log)-1))
		r.applyCommittedLocked()
	}
	return AppendEntriesResponse{term: r.currentTerm, success: true}
}

// Processes a RequestVote RPC and decides whether to grant the vote based on term and log freshness.
func (r *Raft) handleRequestVote(args RequestVoteArgs) RequestVoteResponse {
	if args.term < r.currentTerm {
		return RequestVoteResponse{term: r.currentTerm, voteGranted: false}
	}

	// FIX: votedFor == 0 means "not yet voted this term" (0 is never a valid node id).
	if r.votedFor == 0 || r.votedFor == args.candidateId {
		lastLogIndex := uint64(len(r.log) - 1)
		lastLogTerm := r.log[lastLogIndex].Term
		isUpToDate := args.lastLogTerm > lastLogTerm ||
			(args.lastLogTerm == lastLogTerm && args.lastLogIndex >= lastLogIndex)
		if isUpToDate {
			r.resetTimer()
			r.votedFor = args.candidateId
			return RequestVoteResponse{term: r.currentTerm, voteGranted: true}
		}
	}
	return RequestVoteResponse{term: r.currentTerm, voteGranted: false}
}

type AppendEntriesArgs struct {
	term         uint64
	leaderId     uint64
	prevLogIndex uint64
	prevLogTerm  uint64
	entries      []LogEntry
	leaderCommit uint64
}

type AppendEntriesResponse struct {
	term    uint64
	success bool
}

type RequestVoteArgs struct {
	term         uint64
	candidateId  uint64
	lastLogIndex uint64
	lastLogTerm  uint64
}

type RequestVoteResponse struct {
	term        uint64
	voteGranted bool
}

type AppendEntriesEvent struct {
	args    AppendEntriesArgs
	replyCh chan AppendEntriesResponse
}

type RequestVoteEvent struct {
	args    RequestVoteArgs
	replyCh chan RequestVoteResponse
}

type ClientCommandArgs struct {
	Command Command
}

type ClientCommandResponse struct {
	Success  bool
	LeaderId uint64
	Error    string
}

type ClientCommandEvent struct {
	args    ClientCommandArgs
	replyCh chan ClientCommandResponse
}

type VoteResult struct {
	term    uint64
	granted bool
}

// Sends an AppendEntries RPC to a peer over the configured network.
func (r *Raft) sendAppendEntries(peer uint64, args AppendEntriesArgs) AppendEntriesResponse {
	reply := AppendEntriesResponse{}
	r.net.Call(r.id, peer, "Raft.AppendEntries", args, &reply)
	return reply
}

// Sends a RequestVote RPC to a peer over the configured network.
func (r *Raft) sendRequestVote(peer uint64, args RequestVoteArgs) RequestVoteResponse {
	reply := RequestVoteResponse{}
	r.net.Call(r.id, peer, "Raft.RequestVote", args, &reply)
	return reply
}

// Safely stops and resets the election timeout timer.
func (r *Raft) resetTimer() {
	if !r.electionTimer.Stop() {
		select {
		case <-r.electionTimer.C:
		default:
		}
	}
	r.electionTimer.Reset(r.randomElectionTimeout())
}

type Status struct {
	ID          uint64
	Role        Role
	CurrentTerm uint64
	LeaderId    uint64
	LogLen      int
	CommitIndex uint64
	LastApplied uint64
}

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Returns a snapshot of the node's current role/term/log state.
func (r *Raft) GetStatus() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID:          r.id,
		Role:        r.role,
		CurrentTerm: r.currentTerm,
		LeaderId:    r.leaderId,
		LogLen:      len(r.log),
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
	}
}

// Get returns the current value for key in this node's applied state machine.
func (r *Raft) Get(key string) uint64 {
	return r.state.Get(key)
}

// ID returns the node's id.
func (r *Raft) ID() uint64 {
	return r.id
}
