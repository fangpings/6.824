package raftkv

import (
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

	ID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage map[string]string
	ops     map[int]chan Op
	excuted map[int64]bool
	term    int
}

func (kv *KVServer) setDeamon() {
	for msg := range kv.applyCh {
		appliedOp := msg.Command.(Op) // type assertion for interface
		appliedIndex := msg.CommandIndex
		kv.mu.Lock()
		// 这里再检查一遍是因为提交到raft的log可能也有重复，会被apply两次，所以要检查是否已经提交过
		// 重复的原因可能是这样的：
		// 0. raft中有partition导致了term的变化，leader换人了
		// 1. 我们检查到term变化，所以丢弃了当前所有等待提交的op，报告给clerk让它重传
		// 2. 但实际上前一term的leader已经将我们的log replicate到部分peer上了，这导致新的leader会提交它,但还未报告给kvserver新的提交
		// 3. 但是我们已经通知clerk重传了，clerk也会重新提交一份，kvserver并不会发现这一个op已经apply过，因为raft还未通知
		// 4. 于是raft就提交了两遍相同的op
		// 解决的方法也很简单粗暴，kvserver在apply的时候做一下检查，保证不要重复apply就好了
		if _, ok := kv.excuted[appliedOp.ID]; !ok {
			if appliedOp.Op == "Put" {
				kv.storage[appliedOp.Key] = appliedOp.Value
				kv.excuted[appliedOp.ID] = true
			} else if appliedOp.Op == "Append" {
				val, ok := kv.storage[appliedOp.Key]
				if ok {
					kv.storage[appliedOp.Key] = val + appliedOp.Value
				} else {
					kv.storage[appliedOp.Key] = appliedOp.Value
				}
				kv.excuted[appliedOp.ID] = true
			}
		}
		DPrintf("Server %d, OP %v applied to state machine, current storage %v", kv.me, appliedOp, kv.storage)

		if ch, ok := kv.ops[appliedIndex]; ok {
			ch <- appliedOp
			delete(kv.ops, appliedIndex)
		}
		kv.mu.Unlock()
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
		kv.mu.Lock()
		term, _ := kv.rf.GetState()
		if term != kv.term {
			kv.term = term
			msg := Op{ID: -1}
			for key, ch := range kv.ops {
				ch <- msg
				delete(kv.ops, key)
			}
		}
		kv.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	listenCh := make(chan Op, 1)
	ID := args.ID
	op := Op{"Get", args.Key, "", ID}
	index, _, isLeader := kv.rf.Start(op)
	kv.ops[index] = listenCh

	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	// 一个场景是某个kvserver刚向leader提交了op，leader没有commit就崩溃了
	// 此后没有任何新提交的op了，这时我们不可能通过新提交的op来判断leader变化
	// 只能主动询问自己的peer leadership是否已经变化了
	// (但这是否真的是一个问题？，在partition发生的时候也有可能永远等下去直到partition解决)

	// 本质上我们只在意某一条提交的op是不是永远不会被commit了
	// 能确认这一点的就是本该出现在某一位置的这个op被另一个op占了
	// 所以我们就等着就行了
	DPrintf("Server %d, OP %d submmitted to Raft", kv.me, op.ID%100)
	kv.mu.Unlock()
	appliedOp := <-listenCh
	if appliedOp.ID != ID {
		DPrintf("Server %d, OP %d applied differs from op %d submmitted", kv.me, appliedOp.ID%100, op.ID%100)
		reply.WrongLeader = true
	} else {
		DPrintf("Server %d, OP %d commitment confirmed", kv.me, op.ID%100)
		reply.WrongLeader = false
		kv.mu.Lock()
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	listenCh := make(chan Op, 1)
	ID := args.ID
	if _, ok := kv.excuted[ID]; ok {
		DPrintf("Server %d, op ID %d has been excuted", kv.me, ID)
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	op := Op{args.Op, args.Key, args.Value, ID}
	index, _, isLeader := kv.rf.Start(op)
	kv.ops[index] = listenCh
	kv.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	DPrintf("Server %d, OP %d submmitted to Raft", kv.me, op.ID%100)
	appliedOp := <-listenCh
	if appliedOp.ID != ID {
		DPrintf("Server %d, OP %d applied differs from op %d submmitted", kv.me, appliedOp.ID%100, op.ID%100)
		reply.WrongLeader = true
	} else {
		DPrintf("Server %d, OP %d commitment confirmed", kv.me, op.ID%100)
		reply.WrongLeader = false
		reply.Err = OK
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.storage = map[string]string{}
	kv.ops = map[int]chan Op{}
	kv.excuted = map[int64]bool{}
	kv.term = 0
	go kv.setDeamon()
	go kv.checkTerm()

	// You may need initialization code here.

	return kv
}
