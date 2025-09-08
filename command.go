package casual_raft

type cmdKind uint8

const (
	cmdSet cmdKind = iota
	cmdGet
)

type command struct {
	kind  cmdKind
	key   string
	value string
}
