package casual_raft

// persistentState is the state that MUST BE persisted on all servers and survive crashes
type persistentState struct {
	// currentTerm is the latest term server has seen
	// (initialized to 0 on first boot, increases monotonically)
	currentTerm uint32

	// votedFor marks which candidate did we vote for in the current term
	// 0 == haven't voted yet
	votedFor uint32

	// log is a sequence of commands for state machine
	log []LogEntry
}

// volatileState represents data that can be rebuilt after a crash, kept im memory
type volatileState struct {
	// commitedIndex is the highest log entry known to be commited
	commitedIndex uint32

	// lastApplied is the highest log entry applied to state machine
	lastApplied uint32
}

// leaderState is the data that server tracks about what each follower has replicated
type leaderState struct {
	// nextIndex: for each server, index of the next log entry to send
	// Initialized to (last log index + 1)
	// If append fails, decrement and retry
	nextIndex map[uint32]uint32

	// matchIndex: for each server: highest log entry known to be replicated,
	// Used to determine when entries are commited (majority rule)
	matchIndex map[uint32]uint32
}
