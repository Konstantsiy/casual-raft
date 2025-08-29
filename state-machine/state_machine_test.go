package state_machine

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTest(t *testing.T) {
	var d = []byte{0x00, 0x00, 0x00, 0x03}
	fmt.Println(int(binary.BigEndian.Uint32(d)), string(d))

}

func TestStateMachine_decodeCmd(t *testing.T) {
	var (
		sm stateMachine
		tt = []struct {
			name        string
			msg         []byte
			expectedCmd command
			expectedErr error
		}{
			{
				name:        "set command",
				msg:         []byte{0x00, 0x00, 0x00, 0x00, 0x03, 'k', 'e', 'y', 0x00, 0x00, 0x00, 0x05, 'v', 'a', 'l', 'u', 'e'},
				expectedCmd: command{kind: cmdSet, key: "key", value: "value"},
			},
			{
				name:        "invalid key length",
				msg:         []byte{0x00, 0xFF, 0xFF, 0xFF, 0xFF},
				expectedErr: fmt.Errorf("invalid key length: %d", 4294967295),
			},
			{
				name:        "invalid key length",
				msg:         []byte{0x00, 0xFF, 0xFF, 0xFF, 0xFF},
				expectedErr: fmt.Errorf("invalid key length: %d", 4294967295),
			},
			{
				name:        "message too short for value length",
				msg:         []byte{0x00, 0x00, 0x00, 0x00, 0x03, 'k', 'e', 'y', 0x00, 0x00, 0x00},
				expectedErr: fmt.Errorf("message too short for value length"),
			},
			{
				name:        "invalid value length",
				msg:         []byte{0x00, 0x00, 0x00, 0x00, 0x03, 'k', 'e', 'y', 0xFF, 0xFF, 0xFF, 0xFF},
				expectedErr: fmt.Errorf("invalid value length: %d", 4294967295),
			},
		}
	)

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var res, err = sm.decodeCmd(tc.msg)
			if err != nil {
				require.EqualError(t, err, err.Error(), tc.expectedErr.Error())
			} else {
				require.Equal(t, res, tc.expectedCmd)
			}
		})
	}
}

func TestStateMachine_encodeCmd(t *testing.T) {
	var (
		sm stateMachine
		tt = []struct {
			name        string
			cmd         command
			expectedMsg []byte
			expectedErr error
		}{
			{
				name: "set command",
				cmd:  command{kind: cmdSet, key: "key", value: "value"},
				expectedMsg: []byte{
					0x00,
					0x00, 0x00, 0x00, 0x03,
					'k', 'e', 'y',
					0x00, 0x00, 0x00, 0x05,
					'v', 'a', 'l', 'u', 'e',
				},
			},
			{
				name:        "empty key",
				cmd:         command{kind: cmdSet, key: "", value: "value"},
				expectedMsg: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 'v', 'a', 'l', 'u', 'e'},
			},
			{
				name:        "empty value",
				cmd:         command{kind: cmdSet, key: "key", value: ""},
				expectedMsg: []byte{0x00, 0x00, 0x00, 0x00, 0x03, 'k', 'e', 'y', 0x00, 0x00, 0x00, 0x00},
			},
			{
				name:        "get command",
				cmd:         command{kind: cmdGet, key: "key"},
				expectedMsg: []byte{0x01, 0x00, 0x00, 0x00, 0x03, 'k', 'e', 'y'},
			},
		}
	)

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var res, err = sm.encodeCmd(tc.cmd)
			if err != nil {
				require.EqualError(t, err, tc.expectedErr.Error())
			} else {
				require.Equal(t, res, tc.expectedMsg)
			}
		})
	}
}

func TestStateMachine_encodeDecodeCompatibility(t *testing.T) {
	var (
		sm stateMachine
		tt = []struct {
			name string
			cmd  command
		}{
			{
				name: "set command",
				cmd:  command{kind: cmdSet, key: "key", value: "value"},
			},
			{
				name: "get command",
				cmd:  command{kind: cmdGet, key: "key"},
			},
		}
	)

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := sm.encodeCmd(tc.cmd)
			require.NoError(t, err)

			decoded, err := sm.decodeCmd(encoded)
			require.NoError(t, err)

			require.Equal(t, tc.cmd, decoded)
		})
	}
}
