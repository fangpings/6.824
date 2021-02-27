package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
	appliedOp []interface{}
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	np.appliedOp = ps.appliedOp
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

func (ps *Persister) Log(index int, op interface{}, server int, msg string, val string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if index-1 < len(ps.appliedOp) {
		if ps.appliedOp[index-1] != op {
			DPrintf("Server %d msg %v DIFFERENT LOG WAS APPLIED TO SAME POSITION %d, OLD %v, NEW %v, TOTAL %d", server, msg, index, ps.appliedOp[index-1], op, len(ps.appliedOp))
		}
		return
	} else if index-1 > len(ps.appliedOp) {
		DPrintf("Server %d msg %v MISSING SOME LOG index %d expected %d", server, msg, index, len(ps.appliedOp))
		return
	}
	if val == "" {
		DPrintf("Server %d appending op %v to log at %d", server, op, len(ps.appliedOp))
	} else {
		DPrintf("Server %d appending op %v to log at %d, response %s", server, op, len(ps.appliedOp), val)
	}

	ps.appliedOp = append(ps.appliedOp, op)
}

func (ps *Persister) GetAppliedOp() []interface{} {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.appliedOp
}

func (ps *Persister) SetAppliedOp(appliedOp []interface{}, server int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for i := len(ps.appliedOp); i < len(appliedOp); i++ {
		DPrintf("Server %d appending op %v to log at %d from snapshot", server, appliedOp[i], i)
	}
	ps.appliedOp = appliedOp
}
