package kvsrv

import (
	"testing"

	"6.5840/kvsrv1/rpc"
)

func TestKVServer_Put(t *testing.T) {
	// Arrange
	tests := []struct {
		name          string
		initialDB     map[string]ValueVersion
		args          rpc.PutArgs
		expectedErr   rpc.Err
		expectedValue string
		expectedVer   rpc.Tversion
	}{
		{
			name:          "Insert new key successfully",
			initialDB:     map[string]ValueVersion{},
			args:          rpc.PutArgs{Key: "lockA", Value: "locked", Version: 0},
			expectedErr:   rpc.OK,
			expectedValue: "locked",
			expectedVer:   1,
		},
		{
			name:          "Update existing key successfully",
			initialDB:     map[string]ValueVersion{"lockA": {Value: "unlocked", Version: 1}},
			args:          rpc.PutArgs{Key: "lockA", Value: "locked", Version: 1},
			expectedErr:   rpc.OK,
			expectedValue: "locked",
			expectedVer:   2,
		},
		{
			name:          "Reject update on version mismatch",
			initialDB:     map[string]ValueVersion{"lockA": {Value: "locked", Version: 2}},
			args:          rpc.PutArgs{Key: "lockA", Value: "unlocked", Version: 1}, // Stale version
			expectedErr:   rpc.ErrVersion,
			expectedValue: "locked", // Should not change
			expectedVer:   2,        // Should not change
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: Setup server state
			server := MakeKVServer()
			server.db = tt.initialDB
			reply := &rpc.PutReply{}

			// Act
			server.Put(&tt.args, reply)

			// Assert
			if reply.Err != tt.expectedErr {
				t.Errorf("Expected Err %v, got %v", tt.expectedErr, reply.Err)
			}

			// Verify internal state
			actualValVer := server.db[tt.args.Key]
			if actualValVer.Value != tt.expectedValue {
				t.Errorf("Expected Value %v, got %v", tt.expectedValue, actualValVer.Value)
			}
			if actualValVer.Version != tt.expectedVer {
				t.Errorf("Expected Version %v, got %v", tt.expectedVer, actualValVer.Version)
			}
		})
	}
}
