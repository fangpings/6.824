package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0
const LockTimeDebug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string

	// ID int64
	ClerkIdentifier int64
	ClearkOpIndex   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	storage   map[string]string
	// 这是用来告诉Get或PutAppend goroutine你们请求的Op已经被提交了
	// 我们需要一个公共的goroutine checkApplyCh来检查所有已经被提交的Op
	// 同时针对每一个Get或者PutAppend的请求我们都会有一个专门的goroutine来负责
	// 直到我们直到请求被提交了或者失败之前这些都不能返回
	// 所以我们需要在checkApplyCh和Get/PutAppend之间通信
	// 而且每一条请求都要维护一个ch
	opApplyCh map[int]chan Op
	// excuted     map[int64]bool
	term int
	// for each clerk, we need to know their last request number (this one is monotonically increasing)
	// so if we ever get a smaller request number, we know this is a duplicate
	lastClerkAppliedOpIndex map[int64]int
	lastRaftAppliedLogIndex int

	lockTime time.Time
}

func (kv *KVServer) acquireLock() {
	kv.mu.Lock()
	kv.lockTime = time.Now()
}

func (kv *KVServer) releaseLock(function string) {
	elapsed := time.Since(kv.lockTime)
	kv.mu.Unlock()
	if LockTimeDebug > 0 {
		DPrintf("function %v lock time elapsed: %v", function, elapsed)
	}
}

func (kv *KVServer) checkApplyCh() {
	for msg := range kv.applyCh {
		switch command := msg.Command.(type) {
		case Op:
			clerkIdentifier := command.ClerkIdentifier
			clerkOpIndex := command.ClearkOpIndex   // this is clerk applied ops counter
			raftLogAppliedIndex := msg.CommandIndex // this is raft log applied index
			kv.acquireLock()
			// 这里再检查一遍是因为提交到raft的log可能也有重复，会被apply两次，所以要检查是否已经提交过
			// 重复的原因可能是这样的：
			// 0. raft中有partition导致了term的变化，leader换人了
			// 1. 我们检查到term变化，所以丢弃了当前所有等待提交的op，报告给clerk让它重传
			// 2. 但实际上前一term的leader已经将我们的log replicate到部分peer上了，这导致新的leader会提交它,但还未报告给kvserver新的提交
			// 3. 但是我们已经通知clerk重传了，clerk也会重新提交一份，kvserver并不会发现这一个op已经apply过，因为raft还未通知
			// 4. 于是raft就提交了两遍相同的op
			// 解决的方法也很简单粗暴，kvserver在apply的时候做一下检查，保证不要重复apply就好了
			if _, ok := kv.lastClerkAppliedOpIndex[clerkIdentifier]; !ok {
				kv.lastClerkAppliedOpIndex[clerkIdentifier] = clerkOpIndex - 1
			}
			// DPrintf("Server %d lastRaftAppliedLogIndex %d raftLogAppliedIndex %d", kv.me, kv.lastRaftAppliedLogIndex, raftLogAppliedIndex)
			if kv.lastRaftAppliedLogIndex == raftLogAppliedIndex-1 {
				if clerkOpIndex > kv.lastClerkAppliedOpIndex[clerkIdentifier] {
					if command.Op == "Put" {
						kv.storage[command.Key] = command.Value
					} else if command.Op == "Append" {
						val, ok := kv.storage[command.Key]
						if ok {
							kv.storage[command.Key] = val + command.Value
						} else {
							kv.storage[command.Key] = command.Value
						}
					}
					kv.lastClerkAppliedOpIndex[clerkIdentifier] = clerkOpIndex
				}
				kv.lastRaftAppliedLogIndex = raftLogAppliedIndex
				// kv.checkStateSize()
				// DPrintf("Server %d, OP %v applied to state machine, current storage %v", kv.me, command, kv.storage)
			} else {
				kv.rf.SetLastApplied(kv.lastRaftAppliedLogIndex)
				DPrintf("Server %d, OP %v not applied to state machine", kv.me, command)
			}

			if ch, ok := kv.opApplyCh[raftLogAppliedIndex]; ok {
				ch <- command
				delete(kv.opApplyCh, raftLogAppliedIndex)
			}
			kv.releaseLock("checkApplyCh")
		case raft.InstallSnapshotMsg:
			kv.acquireLock()
			kv.installSnapshot(command.Data)
			kv.releaseLock("installSnapshot")
		}
	}
}

// instruction中提到了解决unreliable的两种方法
// 1. 当op提交的时候，记录提交位置，和rf.Start()报告的位置的op做比较，如果不是同一个说明提交失败，让clerk重新提交
// 2. 检测到term变化，这里要说明一下term变化时我们要丢弃所有等待中的op
// 在partition的时候，如果当前leader未commit，那么partition heal的时候，当前clerk等待的response永远都不会来，因为当前未提交的op
// 已经被新leader自己的log覆盖了，但是第一条的解决方案仅限于有新的op来的时候，如果当前clerk一直没有得到response，那么它就会永远阻塞下去
// 不会有新的op提交，这样就导致问题了
func (kv *KVServer) checkTerm() {
	for {
		kv.acquireLock()
		term, _ := kv.rf.GetState()
		if term != kv.term {
			kv.term = term
			msg := Op{ClerkIdentifier: -1}
			for key, ch := range kv.opApplyCh {
				ch <- msg
				delete(kv.opApplyCh, key)
			}
		}
		kv.releaseLock("checkTerm")
		time.Sleep(500 * time.Millisecond)
	}
}

func (kv *KVServer) checkStateSize() {
	if kv.maxraftstate == -1 {
		// if maxraftstate is -1, we don't need this check
		return
	}
	for {
		if kv.persister.RaftStateSize() > kv.maxraftstate {
			// approaches the threshold, but how close??
			kv.acquireLock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			// 我们不保存opApplyCh的原因是在kvserver崩溃的时候所有goroutine也一同崩溃了
			// 就算我们恢复了他们之间的通信也不会恢复，所以没有保存的必要
			e.Encode(kv.storage)
			e.Encode(kv.lastClerkAppliedOpIndex)
			e.Encode(kv.lastRaftAppliedLogIndex)
			data := w.Bytes()
			kv.releaseLock("checkStateSize")
			kv.rf.Snapshot(data, kv.lastRaftAppliedLogIndex)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *KVServer) installSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// kv.acquireLock()
	// defer kv.releaseLock("installSnapshot")

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storage map[string]string
	var lastAppliedID map[int64]int
	var lastAppliedIndex int

	if e := d.Decode(&storage); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	kv.storage = storage

	if e := d.Decode(&lastAppliedID); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	kv.lastClerkAppliedOpIndex = lastAppliedID

	if e := d.Decode(&lastAppliedIndex); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	kv.lastRaftAppliedLogIndex = lastAppliedIndex

	// for key, ch := range kv.opApplyCh {
	// 	msg := Op{ClerkIdentifier: -1}
	// 	ch <- msg
	// 	delete(kv.opApplyCh, key)
	// }
	// DPrintf("Server %d receive snapshot, content %v, lastAppliedID %d, lastAppliedIndex %d", kv.me, storage, lastAppliedID, lastAppliedIndex)
	kv.rf.SetLastApplied(kv.lastRaftAppliedLogIndex)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	listenCh := make(chan Op, 1)
	// ID := args.ID
	kv.acquireLock()
	op := Op{"Get", args.Key, "", args.Identifier, args.Counter}
	index, _, isLeader := kv.rf.Start(op)
	kv.opApplyCh[index] = listenCh
	kv.releaseLock("Get-2")

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 一个场景是某个kvserver刚向leader提交了op，leader没有commit就崩溃了
	// 此后没有任何新提交的op了，这时我们不可能通过新提交的op来判断leader变化
	// 只能主动询问自己的peer leadership是否已经变化了
	// (但这是否真的是一个问题？，在partition发生的时候也有可能永远等下去直到partition解决)

	// 本质上我们只在意某一条提交的op是不是永远不会被commit了
	// 能确认这一点的就是本该出现在某一位置的这个op被另一个op占了
	// 所以我们就等着就行了
	// DPrintf("Server %d, OP %d submmitted to Raft", kv.me, op.ID%100)

	// Solution: We apply this time out mechanism
	select {
	case <-time.After(time.Duration(1000 * time.Millisecond)):
		kv.mu.Lock()
		delete(kv.opApplyCh, index)
		DPrintf("time out")
		reply.WrongLeader = true
		kv.mu.Unlock()
	case appliedOp := <-listenCh:
		if appliedOp.ClerkIdentifier != args.Identifier || appliedOp.ClearkOpIndex != args.Counter {
			// DPrintf("Server %d, OP %d applied differs from op %d submmitted", kv.me, appliedOp.ID%100, op.ID%100)
			reply.WrongLeader = true
		} else {
			// DPrintf("Server %d, OP %d commitment confirmed", kv.me, op.ID%100)
			reply.WrongLeader = false
			kv.mu.Lock()
			// for any read we need to make sure that one is applied before we actually read anything from storage
			val, ok := kv.storage[appliedOp.Key]
			if ok {
				reply.Value = val
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.acquireLock()
	listenCh := make(chan Op, 1)
	// ID := args.ID
	// Get and PutAppend is different
	// Get is idempotent but PutAppend is not so we need to check if there is duplicate
	if _, ok := kv.lastClerkAppliedOpIndex[args.Identifier]; ok && args.Counter < kv.lastClerkAppliedOpIndex[args.Identifier] {
		// DPrintf("Server %d, op ID %d has been excuted", kv.me, ID)
		reply.WrongLeader = false
		reply.Err = OK
		kv.releaseLock("PutAppend-1")
		return
	}
	op := Op{args.Op, args.Key, args.Value, args.Identifier, args.Counter}
	index, _, isLeader := kv.rf.Start(op)
	kv.opApplyCh[index] = listenCh
	kv.releaseLock("PutAppend-2")

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// DPrintf("Server %d, OP %d submmitted to Raft", kv.me, op.ID%100)
	select {
	case <-time.After(time.Duration(1000 * time.Millisecond)):
		kv.mu.Lock()
		delete(kv.opApplyCh, index)
		DPrintf("time out")
		reply.WrongLeader = true
		kv.mu.Unlock()
	case appliedOp := <-listenCh:
		if appliedOp.ClerkIdentifier != args.Identifier || appliedOp.ClearkOpIndex != args.Counter {
			// DPrintf("Server %d, OP %d applied differs from op %d submmitted", kv.me, appliedOp.ID%100, op.ID%100)
			reply.WrongLeader = true
		} else {
			// DPrintf("Server %d, OP %d commitment confirmed", kv.me, op.ID%100)
			reply.WrongLeader = false
			reply.Err = OK
		}
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	kv.storage = map[string]string{}
	kv.opApplyCh = map[int]chan Op{}
	// kv.excuted = map[int64]bool{}
	kv.lastClerkAppliedOpIndex = map[int64]int{}
	// consider the following scenario:
	// we haven't applied any log, and the raft log is [{"", -1}] (i.e. we only have the default log)
	// then we decide to do a snapshot
	// our next last included index should be lastAppliedIndex - 1, which should be -1
	kv.lastRaftAppliedLogIndex = 0
	kv.term = 0
	kv.installSnapshot(kv.persister.ReadSnapshot())
	go kv.checkApplyCh()
	go kv.checkTerm()
	go kv.checkStateSize()

	// You may need initialization code here.

	return kv
}
