package lock

import (
	"testing"

	"6.5840/kvsrv1/rpc"
)

// MockClerk is a stateful mock that returns predefined sequences of responses.
type MockClerk struct {
	getReplies []getReply
	putReplies []rpc.Err
	getIdx     int
	putIdx     int
	PutCalled  bool
}

type getReply struct {
	val string
	ver rpc.Tversion
	err rpc.Err
}

func (m *MockClerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	reply := m.getReplies[m.getIdx]
	if m.getIdx < len(m.getReplies)-1 {
		m.getIdx++
	}
	return reply.val, reply.ver, reply.err
}

func (m *MockClerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	m.PutCalled = true
	reply := m.putReplies[m.putIdx]
	if m.putIdx < len(m.putReplies)-1 {
		m.putIdx++
	}
	return reply
}

// ---------------------------------------------------------
// TryAcquire Tests
// ---------------------------------------------------------

func TestTryAcquire_BrandNewLock_ReturnsTrue(t *testing.T) {
	// Arrange
	mock := &MockClerk{
		getReplies: []getReply{{val: "", ver: 0, err: rpc.ErrNoKey}},
		putReplies: []rpc.Err{rpc.OK},
	}
	lk := MakeLock(mock, "test-lock")

	// Act
	success := lk.tryAcquire()

	// Assert
	if !success {
		t.Errorf("Expected true for a brand new lock")
	}
}

func TestTryAcquire_HeldBySomeoneElse_ReturnsFalse(t *testing.T) {
	// Arrange
	mock := &MockClerk{
		getReplies: []getReply{{val: "other-user-id", ver: 1, err: rpc.OK}},
	}
	lk := MakeLock(mock, "test-lock")

	// Act
	success := lk.tryAcquire()

	// Assert
	if success {
		t.Errorf("Expected false when lock is held by another ID")
	}
}

func TestTryAcquire_ResolvesErrMaybe_WhenIDMatches(t *testing.T) {
	// Arrange
	mock := &MockClerk{
		// 1st Get: sees unlocked. 2nd Get: sees our ID after the ErrMaybe.
		getReplies: []getReply{
			{val: StateUnlocked, ver: 5, err: rpc.OK},
			{val: "PLACEHOLDER", ver: 6, err: rpc.OK},
		},
		putReplies: []rpc.Err{rpc.ErrMaybe},
	}
	lk := MakeLock(mock, "test-lock")
	mock.getReplies[1].val = lk.id // Inject dynamic ID

	// Act
	success := lk.tryAcquire()

	// Assert
	if !success {
		t.Errorf("Expected true when second Get confirms our ID")
	}
}

// ---------------------------------------------------------
// Release Tests
// ---------------------------------------------------------

func TestRelease_AlreadyUnlocked_ReturnsWithoutPutting(t *testing.T) {
	// Arrange
	mock := &MockClerk{
		getReplies: []getReply{{val: StateUnlocked, ver: 10, err: rpc.OK}},
	}
	lk := MakeLock(mock, "test-lock")

	// Act
	lk.Release()

	// Assert
	if mock.PutCalled {
		t.Errorf("Put should not be called if lock is already unlocked")
	}
}

func TestRelease_LoopsOnErrMaybe_UntilUnlocked(t *testing.T) {
	// Arrange
	mock := &MockClerk{
		// 1st Get: we own it. 2nd Get: confirm unlock succeeded despite ErrMaybe.
		getReplies: []getReply{
			{val: "PLACEHOLDER", ver: 10, err: rpc.OK},
			{val: StateUnlocked, ver: 11, err: rpc.OK},
		},
		putReplies: []rpc.Err{rpc.ErrMaybe},
	}
	lk := MakeLock(mock, "test-lock")
	mock.getReplies[0].val = lk.id

	// Act
	lk.Release()

	// Assert
	if !mock.PutCalled {
		t.Errorf("Put should have been called")
	}
}
