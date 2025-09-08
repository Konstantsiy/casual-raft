package casual_raft

import (
	"encoding/binary"
	"fmt"
	"sync"
)

type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

// stateMachine is a simple in-memory key-value state machine
type stateMachine struct {
	db *sync.Map
}

func (sm *stateMachine) Apply(msg []byte) ([]byte, error) {
	var cmd, err = sm.decodeCmd(msg)
	if err != nil {
		return nil, err
	}

	switch cmd.kind {
	case cmdSet:
		sm.db.Store(cmd.key, cmd.value)

	case cmdGet:
		var value, ok = sm.db.Load(cmd.key)
		if !ok {
			return nil, fmt.Errorf("key not found: %s", cmd.key)
		}

		return []byte(value.(string)), nil
	}

	return nil, nil
}

// decodeCmd decodes a command from a byte slice
/*
	command itself is encoded in bytes as follows:
	[0]     			               - cmdKind
	[1..5] 				   			   - keyLen, uint64
	[5..5+keyLen] 	   	   			   - key
	[5+keyLen..5+keyLen+4] 			   - valueLen, uint64
	[5+keyLen+4 - 5+keyLen+4+valueLen] - value

*/
func (sm stateMachine) decodeCmd(msg []byte) (command, error) {
	var cmd command

	// minimum length is 5 bytes (1 byte for cmdKind and 4 bytes for keyLen)
	if len(msg) < 5 {
		return cmd, fmt.Errorf("commane too short: %d, bytes", len(msg))
	}

	cmd.kind = cmdKind(msg[0])

	var keyLen = int(binary.BigEndian.Uint32(msg[1:5]))
	// limit to 1 KB for safety
	if keyLen <= 0 || keyLen > 1024 {
		return cmd, fmt.Errorf("invalid key length: %d", keyLen)
	}
	// ensure message is long enough for the key
	if len(msg) < 5+keyLen {
		return cmd, fmt.Errorf("incomplete message for key: need %d, got %d", 5+keyLen, len(msg))
	}

	cmd.key = string(msg[5 : 5+keyLen])

	if cmd.kind == cmdSet {
		var valueOffset = 5 + keyLen
		// ensure message is long enough for value length
		if len(msg) < valueOffset+4 {
			return cmd, fmt.Errorf("message too short for value length")
		}

		var valueLen = int(binary.BigEndian.Uint32(msg[valueOffset : valueOffset+4]))
		// limit to 1 MB
		if valueLen < 0 || valueLen > 1024*1024 {
			return cmd, fmt.Errorf("invalid value length: %d", valueLen)
		}
		// ensure message is long enough for the value
		if len(msg) < valueOffset+4+valueLen {
			return cmd, fmt.Errorf("incomplete message for value: need %d, got %d", valueOffset+4+valueLen, len(msg))
		}

		cmd.value = string(msg[valueOffset+4 : valueOffset+4+valueLen])
	}

	return cmd, nil
}

// encodeCmd encodes a command into a byte slice
func (sm stateMachine) encodeCmd(cmd command) ([]byte, error) {
	switch cmd.kind {
	case cmdSet, cmdGet:
	default:
		return nil, fmt.Errorf("unsupported command kind: %d", cmd.kind)
	}

	var keyLen = uint32(len(cmd.key))
	if keyLen == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}
	if keyLen > 1024 {
		return nil, fmt.Errorf("key too large: %d bytes", keyLen)
	}

	var valueLen uint32
	if cmd.kind == cmdSet {
		valueLen = uint32(len(cmd.value))
		if valueLen == 0 {
			return nil, fmt.Errorf("value cannot be empty for SET")
		}
		if valueLen > 1024*1024 {
			return nil, fmt.Errorf("value too large: %d bytes", valueLen)
		}
	}

	// calculate total message length
	var totalMsgLen = 1 + 4 + keyLen
	if cmd.kind == cmdSet {
		totalMsgLen += 4 + valueLen
	}

	buf := make([]byte, totalMsgLen)
	buf[0] = byte(cmd.kind)

	binary.BigEndian.PutUint32(buf[1:5], keyLen)

	copy(buf[5:5+keyLen], cmd.key)

	if cmd.kind == cmdSet {
		var valOffset = 5 + keyLen
		binary.BigEndian.PutUint32(buf[valOffset:valOffset+4], valueLen)

		copy(buf[valOffset+4:valOffset+4+valueLen], cmd.value)
	}

	return buf, nil
}
