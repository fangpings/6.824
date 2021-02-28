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
	Err             string
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
	term      int
	// for each clerk, we need to know their last request number (this one is monotonically increasing)
	// so if we ever get a smaller request number, we know this is a duplicate
	lastClerkAppliedOpIndex map[int64]int
	lastRaftAppliedLogIndex int
}

func (kv *KVServer) checkApplyCh() {
	for msg := range kv.applyCh {
		switch command := msg.Command.(type) {
		case Op:
			clerkIdentifier := command.ClerkIdentifier
			clerkOpIndex := command.ClearkOpIndex   // this is clerk applied ops counter
			raftLogAppliedIndex := msg.CommandIndex // this is raft log applied index
			kv.mu.Lock()
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
			// ret := ""
			if kv.lastRaftAppliedLogIndex == raftLogAppliedIndex-1 {
				new := clerkOpIndex > kv.lastClerkAppliedOpIndex[clerkIdentifier]
				if command.Op == "Get" {
					if val, ok := kv.storage[command.Key]; ok {
						command.Value = val
						// ret = val
					} else {
						command.Err = ErrNoKey
					}
					if new {
						kv.lastClerkAppliedOpIndex[clerkIdentifier] = clerkOpIndex
					}
				} else {
					if new {
						if command.Op == "Put" {
							kv.storage[command.Key] = command.Value
						} else {
							if val, ok := kv.storage[command.Key]; ok {
								kv.storage[command.Key] = val + command.Value
							} else {
								kv.storage[command.Key] = command.Value
							}
						}
						kv.lastClerkAppliedOpIndex[clerkIdentifier] = clerkOpIndex
					}
				}
				kv.lastRaftAppliedLogIndex = raftLogAppliedIndex
				// kv.persister.Log(raftLogAppliedIndex, command, kv.me, "", ret)
				// DPrintf("Server %d, OP %v applied to state machine", kv.me, command)
				if ch, ok := kv.opApplyCh[raftLogAppliedIndex]; ok {
					ch <- command
					delete(kv.opApplyCh, raftLogAppliedIndex)
				}
			} else {
				kv.rf.SetLastApplied(kv.lastRaftAppliedLogIndex)
				// DPrintf("Server %d, index %d OP %v not applied to state machine, expecting index %d", kv.me, raftLogAppliedIndex, command, kv.lastRaftAppliedLogIndex+1)
			}
			kv.mu.Unlock()
		case raft.InstallSnapshotMsg:
			kv.mu.Lock()
			kv.installSnapshot(command.Data)
			kv.mu.Unlock()
		}
	}
}

// instruction中提到了解决unreliable的两种方法
// 1. 当op提交的时候，记录提交位置，和rf.Start()报告的位置的op做比较，如果不是同一个说明提交失败，让clerk重新提交
// 2. 检测到term变化，这里要说明一下term变化时我们要丢弃所有等待中的op
// 在partition的时候，如果当前leader未commit，那么partition heal的时候，当前clerk等待的response永远都不会来，因为当前未提交的op
// 已经被新leader自己的log覆盖了，但是第一条的解决方案仅限于有新的op来的时候，如果当前clerk一直没有得到response，那么它就会永远阻塞下去
// 不会有新的op提交，这样就导致问题了

func (kv *KVServer) checkStateSize() {
	if kv.maxraftstate == -1 {
		// if maxraftstate is -1, we don't need this check
		return
	}
	for {
		// kv.rf.SetLastApplied(kv.lastRaftAppliedLogIndex)
		if kv.persister.RaftStateSize() > kv.maxraftstate {
			// approaches the threshold, but how close??
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			// 我们不保存opApplyCh的原因是在kvserver崩溃的时候所有goroutine也一同崩溃了
			// 就算我们恢复了他们之间的通信也不会恢复，所以没有保存的必要
			e.Encode(kv.storage)
			e.Encode(kv.lastClerkAppliedOpIndex)
			e.Encode(kv.lastRaftAppliedLogIndex)
			// appliedOp := kv.persister.GetAppliedOp()
			// tmp := make([]Op, len(appliedOp))
			// for i := range appliedOp {
			// 	tmp[i] = appliedOp[i].(Op)
			// }
			// e.Encode(tmp)
			data := w.Bytes()
			kv.mu.Unlock()
			kv.rf.Snapshot(data, kv.lastRaftAppliedLogIndex)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *KVServer) installSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	labgob.Register(Op{})
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storage map[string]string
	var lastAppliedID map[int64]int
	var lastAppliedIndex int
	// var appliedOp []Op

	if e := d.Decode(&storage); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	if e := d.Decode(&lastAppliedID); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	if e := d.Decode(&lastAppliedIndex); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	// if e := d.Decode(&appliedOp); e != nil {
	// 	DPrintf("FATAL DECODE ERROR %s", e)
	// 	return
	// }

	// Never roll back storage to an early state
	// what is done is done
	// In tests I see state loss if we allow rolling back to an early state
	if lastAppliedIndex <= kv.lastRaftAppliedLogIndex {
		// last applied should monotonically increase
		return
	}
	// tmp := make([]interface{}, len(appliedOp))
	// for i := range appliedOp {
	// 	tmp[i] = appliedOp[i]
	// }
	// kv.persister.SetAppliedOp(tmp, kv.me)
	kv.storage = storage
	kv.lastClerkAppliedOpIndex = lastAppliedID
	kv.lastRaftAppliedLogIndex = lastAppliedIndex
	kv.rf.SetLastApplied(kv.lastRaftAppliedLogIndex)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	listenCh := make(chan Op, 1)
	// ID := args.ID
	kv.mu.Lock()
	op := Op{"Get", args.Key, "", args.Identifier, args.Counter, ""}
	index, _, isLeader := kv.rf.Start(op)
	kv.opApplyCh[index] = listenCh
	kv.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 一个场景是某个kvserver刚向leader提交了op，leader没有commit就崩溃了
	// 此后没有任何新提交的op了，这时我们不可能通过新提交的op来判断leader变化
	// 只能主动询问自己的peer leadership是否已经变化了
	// (但这是否真的是一个问题？，在partition发生的时候也有可能永远等下去直到partition解决)
	// Solution: We apply this time out mechanism
	select {
	case <-time.After(time.Duration(1000 * time.Millisecond)):
		kv.mu.Lock()
		delete(kv.opApplyCh, index)
		reply.WrongLeader = true
		kv.mu.Unlock()
	case appliedOp := <-listenCh:
		if appliedOp.ClerkIdentifier != args.Identifier || appliedOp.ClearkOpIndex != args.Counter {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			if appliedOp.Err == ErrNoKey {
				reply.Err = ErrNoKey
			} else {
				reply.Value = appliedOp.Value
				reply.Err = OK
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	listenCh := make(chan Op, 1)
	if _, ok := kv.lastClerkAppliedOpIndex[args.Identifier]; ok && args.Counter < kv.lastClerkAppliedOpIndex[args.Identifier] {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	op := Op{args.Op, args.Key, args.Value, args.Identifier, args.Counter, ""}
	index, _, isLeader := kv.rf.Start(op)
	kv.opApplyCh[index] = listenCh
	kv.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	select {
	case <-time.After(time.Duration(1000 * time.Millisecond)):
		kv.mu.Lock()
		delete(kv.opApplyCh, index)
		// DPrintf("time out")
		reply.WrongLeader = true
		kv.mu.Unlock()
	case appliedOp := <-listenCh:
		if appliedOp.ClerkIdentifier != args.Identifier || appliedOp.ClearkOpIndex != args.Counter {
			reply.WrongLeader = true
		} else {
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
	kv.lastClerkAppliedOpIndex = map[int64]int{}
	// consider the following scenario:
	// we haven't applied any log, and the raft log is [{"", -1}] (i.e. we only have the default log)
	// then we decide to do a snapshot
	// our next last included index should be lastAppliedIndex - 1, which should be -1
	kv.lastRaftAppliedLogIndex = 0
	kv.term = 0
	kv.installSnapshot(kv.persister.ReadSnapshot())
	go kv.checkApplyCh()
	go kv.checkStateSize()

	// You may need initialization code here.

	return kv
}
