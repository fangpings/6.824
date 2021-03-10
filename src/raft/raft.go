package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// import "sync"
// import "labrpc"

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	follower = iota
	candidate
	leader
	killed
)

type logEntry struct {
	Command interface{}
	Term    int
}

type InstallSnapshotMsg struct {
	Data []byte
}

const LockTimeDebug = 0

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	currentTerm int
	votedFor    int
	log         []logEntry

	// volatile for all states
	commitIndex int
	lastApplied int

	//volatile for leader only
	nextIndex  []int
	matchIndex []int

	// other parameter
	electionTimeout   time.Duration
	lastUpdate        time.Time
	state             int
	sleepTime         time.Duration
	numPeers          int
	majority          int
	applyCh           chan ApplyMsg
	lastIncludedIndex int // last index included in the snapshot (NOT in current log)
	lastIncludedTerm  int

	lockTime       time.Time
	lastAppendTime time.Time
}

func (rf *Raft) acquireLock() {
	rf.mu.Lock()
	rf.lockTime = time.Now()
}

func (rf *Raft) releaseLock(function string) {
	elapsed := time.Since(rf.lockTime)
	if LockTimeDebug > 0 {
		DPrintf("function %v lock time elapsed: %v", function, elapsed)
	}
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

func (rf *Raft) Snapshot(snapshot []byte, lastAppliedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("term %d id %d last applied index: %d, last snapshot index: %d, last log: %d", rf.currentTerm, rf.me, lastAppliedIndex, rf.lastIncludedIndex, rf.lastTotalLogIndex())
	// total log:      0 1 2 3(last included) | 4 5 6 (last applied) 7 8
	// current log:                           | 0 1 2                3 4
	// we need to keep last applied log, otherwise we might have empty log, which will incur some problem
	if rf.lastIncludedIndex >= lastAppliedIndex || lastAppliedIndex > rf.lastTotalLogIndex() {
		return
	}
	rf.lastIncludedTerm = rf.getLogTermByTotalIndex(lastAppliedIndex)
	rf.log = rf.log[lastAppliedIndex-rf.lastIncludedIndex:] // we have to keep last log entry
	rf.lastIncludedIndex = lastAppliedIndex
	// if rf.lastApplied < lastAppliedIndex {
	// 	rf.lastApplied = lastAppliedIndex
	// }
	logData := rf.makePersistData()
	rf.persister.SaveStateAndSnapshot(logData, snapshot)
	if rf.commitIndex > rf.lastTotalLogIndex() {
		DPrintf("term %d id %d commit index %d leads current log %d b", rf.currentTerm, rf.me, rf.commitIndex, rf.lastTotalLogIndex())
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) makePersistData() (data []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	// persist doesn't need locks, MAKE SURE that this method is called only within critical region
	rf.persister.SaveRaftState(rf.makePersistData())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if e := d.Decode(&currentTerm); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	rf.currentTerm = currentTerm

	if e := d.Decode(&votedFor); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	rf.votedFor = votedFor

	if e := d.Decode(&log); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	rf.log = log

	if e := d.Decode(&lastIncludedIndex); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	rf.lastIncludedIndex = lastIncludedIndex

	if e := d.Decode(&lastIncludedTerm); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	rf.lastIncludedTerm = lastIncludedTerm
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	// Assume that only followers will receive this rpc call
	// because this rpc will be called only if leader finds out
	// that the follower is lagging too much
	// Otherwise we have to deal with state problem
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if rf.state == leader {
		DPrintf("LEADER RECEIVE SNAPSHOT!!!")
	}

	// we have to keep this snapshot anyways
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	correspondingIndex := rf.total2CurrentLogIndex(args.LastIncludedIndex)
	if correspondingIndex <= len(rf.log)-1 &&
		correspondingIndex >= 0 &&
		rf.getLogTermByCurrentIndex(correspondingIndex) == args.LastIncludedTerm {
		// we have the log of last snap shot, discard everything before that log and retain the rest
		if correspondingIndex == len(rf.log)-1 {
			rf.log = make([]logEntry, 0)
		} else {
			rf.log = rf.log[correspondingIndex:]
		}
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.log = make([]logEntry, 0)
	msg := ApplyMsg{false, InstallSnapshotMsg{args.Data}, -1}
	logData := rf.makePersistData()
	rf.persister.SaveStateAndSnapshot(logData, args.Data)
	rf.mu.Unlock()
	// applyCh will block so lock must be released
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int) {
	// DPrintf("term %d id %d send install snapshot to sever %d", rf.currentTerm, rf.me, server)
	for {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		lastIncludedIndex := rf.lastIncludedIndex
		req := &InstallSnapshotArgs{
			rf.currentTerm,
			rf.me,
			rf.lastIncludedIndex,
			rf.lastIncludedTerm,
			rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		rep := &InstallSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", req, rep)
		if ok {
			rf.mu.Lock()
			if rep.Term > rf.currentTerm {
				rf.currentTerm = rep.Term
				rf.votedFor = -1
				rf.state = follower
				rf.mu.Unlock()
				go rf.onBecomingFollower()
				return
			}
			// if send snapshot succeeded, we need to update as well
			if rf.nextIndex[server] < lastIncludedIndex+1 {
				rf.nextIndex[server] = lastIncludedIndex + 1
			}
			rf.mu.Unlock()
			go rf.sendAppendEntries(server)
			return
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// leader 和 candidate在当前term肯定都投给自己了，不用像AppendEntry一样做很多检查
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state != follower {
			go rf.onBecomingFollower()
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// check term
	if args.Term < rf.currentTerm {
		return
	}
	// check whether this term I have voted to another candidate
	if !(rf.votedFor == -1 || rf.votedFor == args.CandidateID) {
		return
	}
	// 这条检查在原本的raft paper中是没有的
	// 但是我们必须要加入这条检查
	// 原因是当我们加入了snapshot之后，会存在一个问题
	// 就是commitIndex有时候会大于自己最大的log index
	// 我们对commitIndex的修改是非常严格的,即只会增加，不会减少
	// 所以有时候可能会在收到snapshot之后导致log落后于commitIndex
	// 当然这可能也可以通过拒绝那个snapshot实现
	// 不过我们没有这样解决
	// 带来的问题就是在投票的时候，有些log落后的candidate会钻这个空子
	// 即某个candidate的log实际上是落后于commit的
	// 即实际上有些log已经commit了，但是凑巧有些server上面并没有这些log
	// 而这个candidate也没有，所以他阴差阳错就当选了
	// 当选之后他就会覆盖这些已经commit的log，这就会导致log inconsistency
	if args.LastLogIndex < rf.commitIndex {
		return
	}
	if args.LastLogTerm > rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.lastTotalLogIndex()) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.lastUpdate = time.Now()
		// DPrintf("term %d id %d vote grated to %d, my last index %d term %d, his last index %d term %d", rf.currentTerm, rf.me, args.CandidateID, rf.lastTotalLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm)
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, okChan chan int) {
	rf.mu.Lock()
	req := &RequestVoteArgs{rf.currentTerm, rf.me, rf.lastTotalLogIndex(), rf.getLastLogTerm()}
	// DPrintf("term %d id %d sending request vote to %d request %v", rf.currentTerm, rf.me, server, req)
	rf.mu.Unlock()
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", req, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != candidate || reply.Term < rf.currentTerm || req.Term < rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = follower
			go rf.onBecomingFollower()
			return
		}
		if reply.VoteGranted {
			okChan <- 1
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// DPrintf("term %d id %d receive append entries %v ", rf.currentTerm, rf.me, args)

	rf.lastUpdate = time.Now()

	// 如果当前状态是follower, 且检查到term > currentTerm, 那么直接更新term
	// 如果当前状态是leader, 那么term不可能撞车的（根据paper，同一term只会选出一个leader），所以检查到term > currentTerm，重新回到follower就行
	// 如果当前状态是candidate，那么有两种情况，第一种情况是当前term有人选成leader了，这个时候直接变成follower（不要更新votedFor和term），第二种情况是收到term更高的leader了，这个时候直接变成follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// 此处必须要先变成follower才行
		// 考虑以下情形：
		// AppendEntries发现term落后，修改term, spawn onBecomingFollower() 但是没有修改state
		// onBecomingFollower()需要等待锁，此时可能sendAppendEntries()也在等待锁
		// sendAppendEntries()得到锁，并没有发现state变化，但是term已经变化了，实际上已经不再是leader
		// 这样就可能导致当前term有两个实质上的leader
		rf.state = follower
		rf.votedFor = -1
		if rf.state != follower {
			go rf.onBecomingFollower()
		}
	} else if args.Term == rf.currentTerm && rf.state == candidate {
		rf.state = follower
		go rf.onBecomingFollower()
	}
	reply.Term = rf.currentTerm

	// what if PrevLogIndex is something we have already snapshotted?
	if args.Term < rf.currentTerm ||
		rf.lastTotalLogIndex() < args.PrevLogIndex ||
		rf.lastIncludedIndex > args.PrevLogIndex ||
		rf.getLogTermByTotalIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		return
	}
	reply.Success = true

	// 论文上说是确定match之后Append any new entries not already in the log
	// 但是我们的操作是每次leader会发送从prevIndex到最新的所有log给follower
	// 所以我们确定match之后从match的index直接覆盖所有就行了
	// if log contains all the entries that leader sent, we should not truncate it
	// because we might drop the entires that follows
	if rf.lastTotalLogIndex() < args.PrevLogIndex+len(args.Entries) ||
		(len(args.Entries) > 0 &&
			rf.getLogTermByTotalIndex(args.PrevLogIndex+len(args.Entries)) != args.Entries[len(args.Entries)-1].Term) {
		rf.log = append(rf.log[:rf.total2CurrentLogIndex(args.PrevLogIndex+1)], args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	for {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		lastIndex := rf.lastTotalLogIndex()
		// total log:   0 1 2 3 4(last snapshot) 5 6 7(next_index) 8
		// current log:                          0 1 2             3

		// if -1, it means we need last included log in snapshot
		// we do not have to send the snapshot at this time because
		// we know the term of that log
		// only when we need information before this log entry do we
		// need to send install snapshot
		if rf.total2CurrentLogIndex(rf.nextIndex[server]-1) < -1 {
			// this means follower is lagging behind
			// log for nextIndex has been snapshotted by leader
			rf.mu.Unlock()
			go rf.sendInstallSnapshot(server)
			return
		}
		var entries []logEntry
		if len(rf.log) == 0 {
			entries = rf.log
		} else {
			entries = rf.log[rf.total2CurrentLogIndex(rf.nextIndex[server]):]
		}
		DPrintf("leader sends %v", entries)
		req := &AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			rf.nextIndex[server] - 1,
			rf.getLogTermByTotalIndex(rf.nextIndex[server] - 1),
			entries,
			rf.commitIndex,
		}
		rf.mu.Unlock()

		rep := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", req, rep)
		if ok {
			rf.mu.Lock()

			// IMPORTANT! if other sendAppendEntries goroutines detect that the leader's term lags
			// they will set leader's state to follower, but other goroutines don't know this
			// so they will receive the reply and continue to communicate with followers
			// but its term has been updated by that goroutine who first detects this
			// so other goroutines cannot quit properly and will continue to decrease nextIndex until it reaches 0
			// 如果rep的term已经落后了，那么我们不应该继续了,但是这一条似乎没有显示出效果
			if rf.state != leader || rep.Term < rf.currentTerm || req.Term < rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			if rep.Term > rf.currentTerm {
				rf.currentTerm = rep.Term
				rf.votedFor = -1
				rf.state = follower
				rf.mu.Unlock()
				go rf.onBecomingFollower()
				return
			}
			if rep.Success {
				if lastIndex+1 > rf.nextIndex[server] {
					rf.nextIndex[server] = lastIndex + 1
				}
				if lastIndex > rf.matchIndex[server] {
					rf.matchIndex[server] = lastIndex
				}
				for i := rf.commitIndex + 1; i <= rf.lastTotalLogIndex(); i++ {
					if i < rf.lastIncludedIndex || rf.getLogTermByTotalIndex(i) != rf.currentTerm {
						continue
					}
					count := 1
					for j := 0; j < rf.numPeers; j++ {
						if j == rf.me {
							continue
						}
						if rf.matchIndex[j] >= i {
							count++
						}
					}

					// A leader is not allowed to update commitIndex to somewhere in a previous term
					//(or, for that matter, a future term).
					// Thus, as the rule says, you specifically need to check that log[N].term == currentTerm.
					// This is because Raft leaders cannot be sure an entry is actually committed (and will not ever be changed in the future)
					// if it’s not from their current term. This is illustrated by Figure 8 in the paper.

					if count >= rf.majority {
						rf.commitIndex = i
					}
				}

				rf.mu.Unlock()
				return
			}

			if rf.nextIndex[server] >= 1 {
				rf.nextIndex[server]--
			}
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		rf.log = append(rf.log, logEntry{command, rf.currentTerm})
		rf.persist()
		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntries(i)
		}
		DPrintf("term %d id %d receiving log %v at %d", rf.currentTerm, rf.me, command, rf.lastTotalLogIndex())
	}

	return rf.lastTotalLogIndex(), rf.currentTerm, rf.state == leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

	// this is for sequential testing
	// sometimes some goroutines from previous tests are not killed thoroughly
	rf.state = killed
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers // including self
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.numPeers = len(rf.peers)
	rf.majority = int(rf.numPeers/2) + 1
	rf.lastUpdate = time.Now()

	rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
	// rf.sleepTime = time.Duration(100) * time.Millisecond
	// when we start we doesn't include any log in our snapshot
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]logEntry, 1)
	rf.log[0] = logEntry{"", -1}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.onBecomingFollower()
	go rf.checkingCommit()
	go rf.reportingState()

	rf.lastAppendTime = time.Now()

	return rf
}

func (rf *Raft) onBecomingFollower() {
	rf.mu.Lock()
	// DPrintf("term %d id %d becoming follower", rf.currentTerm, rf.me)
	rf.state = follower
	rf.lastUpdate = time.Now()
	rf.persist()
	rf.mu.Unlock()

	for {
		if rf.state != follower {
			return
		}
		rf.mu.Lock()
		if time.Since(rf.lastUpdate) > rf.electionTimeout {
			go rf.onBecomingCandidate()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// previously we set this to 100, which is too long !
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) onBecomingCandidate() {
	// DPrintf("term %d id %d becoming candidate", rf.currentTerm, rf.me)
	rf.state = candidate

	for {
		rf.mu.Lock()
		if rf.state != candidate {
			rf.mu.Unlock()
			// DPrintf("term %d id %d quitting candidate 1", rf.currentTerm, rf.me)
			return
		}
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		rf.mu.Unlock()

		okChan := make(chan int, rf.numPeers)
		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me {
				continue
			}
			go rf.sendRequestVote(i, okChan)
		}

		// we cannot just sleep the whole election timeout and count the vote
		// because it will cause other followers who grant votes timeout and becomes candidate
		func(okChan chan int) {
			count := 1
			for {
				select {
				case <-okChan:
					rf.mu.Lock()
					count++
					if count >= rf.majority && rf.state == candidate {
						rf.mu.Unlock()
						go rf.onBecomingLeader()
						return
					}
					rf.mu.Unlock()
				case <-time.After(rf.electionTimeout):
					return
				}
			}
		}(okChan)
	}
}

func (rf *Raft) onBecomingLeader() {
	rf.mu.Lock()
	DPrintf("term %d id %d IS ELECTED LEADER!!!", rf.currentTerm, rf.me)
	rf.state = leader
	rf.nextIndex = make([]int, rf.numPeers)
	for i := 0; i < rf.numPeers; i++ {
		rf.nextIndex[i] = rf.lastTotalLogIndex() + 1
	}
	rf.matchIndex = make([]int, rf.numPeers)
	rf.mu.Unlock()

	for {
		if rf.state != leader {
			return
		}

		// 另一个尝试是将sendAppendEntries 做成每个server对应的独立循环goroutine
		// 类似于
		// for { ok = sendAppendEntriesRPC; if ok { update; sleep;} else {resend immediately}}
		// 这样我们只需要在成为leader的时候开始全部的goroutine
		// 而在leader的主goroutine中统计commit即可
		// 但这样做是有问题的，问题在于rpc call是block的。一旦遇到网络问题
		// 我们可能时隔很久才会收到reply,这样的话我们就会导致client timeout
		// 进而导致不断重选
		// 所以我们必须要在主leader goroutine中定时循环，这样我们不会因为网络问题被block导致重选
		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me {
				continue
			}
			// raft rpcs are idempotent
			// so even if there is a partition and leader is repeated sending
			// same AppendEntries RPC and InstallSnapshot RPC it should be fine
			// TODO: Why don't we make sendAppendEntries go once when we becomes leader?
			go rf.sendAppendEntries(i)
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) checkingCommit() {
	// we don't have locks here because applyCh is blocking
	for {
		if rf.state == killed {
			return
		}
		rf.mu.Lock()
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}
		if rf.commitIndex < rf.lastApplied {
			rf.commitIndex = rf.lastApplied
		}
		msgs := make([]ApplyMsg, 0)
		commitIndex := rf.commitIndex
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			current := rf.total2CurrentLogIndex(i)
			if current < 0 || current > len(rf.log)-1 {
				break
			}
			msg := ApplyMsg{true, rf.log[rf.total2CurrentLogIndex(i)].Command, i}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}

		rf.mu.Lock()
		rf.lastApplied = commitIndex
		rf.persist()
		rf.mu.Unlock()

		time.Sleep(time.Duration(1) * time.Millisecond)
	}
}

func (rf *Raft) SetLastApplied(lastApplied int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = lastApplied
}

// when use these methods, make sure rf.lastIncludedIndex is unmodified
func (rf *Raft) current2TotalLogIndex(currentIndex int) int {
	return rf.lastIncludedIndex + currentIndex + 1
}

func (rf *Raft) total2CurrentLogIndex(totalIndex int) int {
	return totalIndex - rf.lastIncludedIndex - 1
}

func (rf *Raft) lastTotalLogIndex() int {
	return rf.current2TotalLogIndex(len(rf.log) - 1)
}

func (rf *Raft) getLogTermByCurrentIndex(currentIndex int) int {
	if currentIndex == -1 {
		return rf.lastIncludedTerm
	}
	return rf.log[currentIndex].Term
}

func (rf *Raft) getLogTermByTotalIndex(totalIndex int) int {
	return rf.getLogTermByCurrentIndex(rf.total2CurrentLogIndex(totalIndex))
}

func (rf *Raft) getLastLogTerm() int {
	return rf.getLogTermByCurrentIndex(len(rf.log) - 1)
}

func (rf *Raft) reportingState() {
	for {
		rf.mu.Lock()
		// switch rf.state {
		// case follower:
		// 	DPrintf("term %d id %d in state follower commit index %d", rf.currentTerm, rf.me, rf.commitIndex)
		// case candidate:
		// 	DPrintf("term %d id %d in state candidate commit index %d", rf.currentTerm, rf.me, rf.commitIndex)
		// case leader:
		// 	DPrintf("term %d id %d in state leader commit index %d", rf.currentTerm, rf.me, rf.commitIndex)
		// }
		DPrintf("term %d id %d current log %v", rf.currentTerm, rf.me, rf.log)
		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}
