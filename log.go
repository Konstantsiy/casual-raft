package casual_raft

type LogEntry struct {
	Index   uint32 // log index starting from 1
	Term    uint32 // term when entry was received by leader
	Command []byte // command for state machine
}
