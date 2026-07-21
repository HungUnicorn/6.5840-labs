package shardctrler

import (
	"testing"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)

// ---------------------------------------------------------
// MockClerk
// ---------------------------------------------------------

type mockClerk struct {
	getReplies []getReply
	putReplies []rpc.Err
	getIdx     int
	putIdx     int

	putCalls []putCall
}

type getReply struct {
	val string
	ver rpc.Tversion
	err rpc.Err
}

type putCall struct {
	key     string
	value   string
	version rpc.Tversion
}

func (m *mockClerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	reply := m.getReplies[m.getIdx]
	if m.getIdx < len(m.getReplies)-1 {
		m.getIdx++
	}
	return reply.val, reply.ver, reply.err
}

func (m *mockClerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	m.putCalls = append(m.putCalls, putCall{key, value, version})
	reply := m.putReplies[m.putIdx]
	if m.putIdx < len(m.putReplies)-1 {
		m.putIdx++
	}
	return reply
}

func makeTestConfig(num shardcfg.Tnum) *shardcfg.ShardConfig {
	cfg := shardcfg.MakeShardConfig()
	cfg.Num = num
	cfg.Groups[shardcfg.Gid1] = []string{"s1"}
	for i := 0; i < shardcfg.NShards; i++ {
		cfg.Shards[i] = shardcfg.Gid1
	}
	return cfg
}

func makeCtrlerWithMock(mock *mockClerk) *ShardCtrler {
	return &ShardCtrler{IKVClerk: mock}
}

// ---------------------------------------------------------
// tryClaimNextConfig Tests
// ---------------------------------------------------------

func TestTryClaimNextConfig_PutOK_ReturnsClaimed(t *testing.T) {
	mock := &mockClerk{
		putReplies: []rpc.Err{rpc.OK},
	}
	sck := makeCtrlerWithMock(mock)
	cfg := makeTestConfig(2)

	result := sck.tryClaimNextConfig(cfg)

	if result != claimed {
		t.Errorf("Expected claimed, got %v", result)
	}
	if len(mock.putCalls) != 1 {
		t.Fatalf("Expected 1 Put call, got %d", len(mock.putCalls))
	}
	if mock.putCalls[0].key != "next_config_2" {
		t.Errorf("Expected key next_config_2, got %s", mock.putCalls[0].key)
	}
	if mock.putCalls[0].version != 0 {
		t.Errorf("Expected version 0 (create-if-not-exists), got %d", mock.putCalls[0].version)
	}
}

func TestTryClaimNextConfig_PutErrVersion_ReturnsNotClaimed(t *testing.T) {
	mock := &mockClerk{
		putReplies: []rpc.Err{rpc.ErrVersion},
	}
	sck := makeCtrlerWithMock(mock)
	cfg := makeTestConfig(2)

	result := sck.tryClaimNextConfig(cfg)

	if result != notClaimed {
		t.Errorf("Expected notClaimed when another controller already claimed, got %v", result)
	}
}

func TestTryClaimNextConfig_ErrMaybe_OwnWriteConfirmed_ReturnsClaimed(t *testing.T) {
	cfg := makeTestConfig(2)
	mock := &mockClerk{
		putReplies: []rpc.Err{rpc.ErrMaybe},
		getReplies: []getReply{
			{val: cfg.String(), ver: 1, err: rpc.OK},
		},
	}
	sck := makeCtrlerWithMock(mock)

	result := sck.tryClaimNextConfig(cfg)

	if result != claimed {
		t.Errorf("Expected claimed when re-read confirms own write, got %v", result)
	}
}

func TestTryClaimNextConfig_ErrMaybe_DifferentConfigStored_ReturnsNotClaimed(t *testing.T) {
	cfg := makeTestConfig(2)
	otherCfg := makeTestConfig(2)
	otherCfg.Groups[tester.Tgid(99)] = []string{"other-server"}

	mock := &mockClerk{
		putReplies: []rpc.Err{rpc.ErrMaybe},
		getReplies: []getReply{
			{val: otherCfg.String(), ver: 1, err: rpc.OK},
		},
	}
	sck := makeCtrlerWithMock(mock)

	result := sck.tryClaimNextConfig(cfg)

	if result != notClaimed {
		t.Errorf("Expected notClaimed when re-read shows a different controller's config, got %v", result)
	}
}

// ---------------------------------------------------------
// needsMove Tests
// ---------------------------------------------------------

func TestNeedsMove_SameGroup_ReturnsFalse(t *testing.T) {
	if needsMove(tester.Tgid(1), tester.Tgid(1)) {
		t.Errorf("Same src and dst group should not need a move")
	}
}

func TestNeedsMove_SrcUnassigned_ReturnsFalse(t *testing.T) {
	if needsMove(tester.Tgid(0), tester.Tgid(1)) {
		t.Errorf("Unassigned src should not need a move")
	}
}

func TestNeedsMove_DstUnassigned_ReturnsFalse(t *testing.T) {
	if needsMove(tester.Tgid(1), tester.Tgid(0)) {
		t.Errorf("Unassigned dst should not need a move")
	}
}

func TestNeedsMove_DifferentAssignedGroups_ReturnsTrue(t *testing.T) {
	if !needsMove(tester.Tgid(1), tester.Tgid(2)) {
		t.Errorf("Different assigned groups should need a move")
	}
}

// ---------------------------------------------------------
// nextConfigKey Tests
// ---------------------------------------------------------

func TestNextConfigKey_FormatsCorrectly(t *testing.T) {
	tests := []struct {
		num  shardcfg.Tnum
		want string
	}{
		{1, "next_config_1"},
		{5, "next_config_5"},
		{100, "next_config_100"},
	}
	for _, tt := range tests {
		got := nextConfigKey(tt.num)
		if got != tt.want {
			t.Errorf("nextConfigKey(%d) = %q, want %q", tt.num, got, tt.want)
		}
	}
}
