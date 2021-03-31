package shardmaster

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"reflect"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs                 []Config // indexed by config num
	opApplyCh               map[int]chan Reply
	lastClerkAppliedOpIndex map[int64]int
	lastRaftAppliedLogIndex int
}

type Op struct {
	// Your data here.
	Args            interface{}
	Reply           interface{}
	ClerkIdentifier int64
	ClearkOpIndex   int
}

type Reply struct {
	BaseReply
	clerkIdentifier int64
	clearkOpIndex   int
	config          Config
}

const Debug = 1

func DPrintf(me int, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 && me == 0 {
		log.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) checkApplyCh() {
	for msg := range sm.applyCh {
		raftLogAppliedIndex := msg.CommandIndex
		op := msg.Command.(Op)
		clerkIdentifier := op.ClerkIdentifier
		clerkOpIndex := op.ClearkOpIndex
		sm.mu.Lock()
		if _, ok := sm.lastClerkAppliedOpIndex[clerkIdentifier]; !ok {
			sm.lastClerkAppliedOpIndex[clerkIdentifier] = clerkOpIndex - 1
		}
		if sm.lastRaftAppliedLogIndex == raftLogAppliedIndex-1 {
			new := clerkOpIndex > sm.lastClerkAppliedOpIndex[clerkIdentifier]
			reply := Reply{clerkIdentifier: clerkIdentifier, clearkOpIndex: clerkOpIndex}
			// oldconfig := sm.configs[len(sm.configs)-1]
			switch op.Args.(type) {
			case JoinArgs:
				if new {
					reply.BaseReply.Err = sm.processJoin(op.Args.(JoinArgs).Servers)
					DPrintf(sm.me, "MASTER ID %d join %v", sm.me, op.Args.(JoinArgs).Servers)
				}
				break
			case LeaveArgs:
				if new {
					reply.BaseReply.Err = sm.processLeave(op.Args.(LeaveArgs).GIDs)
					DPrintf(sm.me, "MASTER ID %d leave %v", sm.me, op.Args.(LeaveArgs).GIDs)
				}
				break
			case MoveArgs:
				if new {
					reply.BaseReply.Err = sm.processMove(op.Args.(MoveArgs).Shard, op.Args.(MoveArgs).GID)
				}
				break
			case QueryArgs:
				err, config := sm.processQuery(op.Args.(QueryArgs).Num)
				reply.BaseReply.Err = err
				reply.config = config
			default:
				DPrintf(sm.me, "WARNING UNSUPPORTED OP TYPE %v", reflect.TypeOf(op.Args))
			}
			sm.lastRaftAppliedLogIndex = raftLogAppliedIndex
			// DPrintf(sm.me, "ID %d apply command type %v %v\n last config %v\n last config %v", sm.me, reflect.TypeOf(op.Args), op.Args, oldconfig, sm.configs[len(sm.configs)-1])
			if ch, ok := sm.opApplyCh[raftLogAppliedIndex]; ok {
				// DPrintf("ID %d reply %v", sm.me, reply)
				ch <- reply
				delete(sm.opApplyCh, raftLogAppliedIndex)
			}
		} else {
			sm.rf.SetLastApplied(sm.lastRaftAppliedLogIndex)
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) process(args interface{}, identifier int64, counter int) Reply {
	sm.mu.Lock()
	reply := Reply{}
	if _, ok := sm.lastClerkAppliedOpIndex[identifier]; ok && counter < sm.lastClerkAppliedOpIndex[identifier] {
		reply.BaseReply.WrongLeader = false
		reply.BaseReply.Err = OK
		sm.mu.Unlock()
		return reply
	}
	op := Op{args, nil, identifier, counter}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.BaseReply.WrongLeader = true
		sm.mu.Unlock()
		return reply
	}
	listenCh := make(chan Reply, 1)
	sm.opApplyCh[index] = listenCh
	sm.mu.Unlock()

	select {
	case <-time.After(time.Duration(1000 * time.Millisecond)):
		sm.mu.Lock()
		delete(sm.opApplyCh, index)
		reply.BaseReply.WrongLeader = true
		sm.mu.Unlock()
	case reply = <-listenCh:
		if reply.clerkIdentifier != identifier || reply.clearkOpIndex != counter {
			reply.BaseReply.WrongLeader = true
		} else {
			reply.BaseReply.WrongLeader = false
			reply.BaseReply.Err = OK
		}
	}
	return reply
}

func (sm *ShardMaster) processJoin(servers map[int][]string) Err {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1}
	newConfig.Groups = make(map[int][]string)
	// if len(servers)+len(lastConfig.Groups) > NShards {
	// 	return TooManyGroups
	// }
	if lastConfig.Num == 0 {
		groups := make([]int, 0)
		for k, v := range servers {
			newConfig.Groups[k] = v
			groups = append(groups, k)
		}
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = groups[i%len(groups)]
		}
	} else {
		for k, v := range lastConfig.Groups {
			newConfig.Groups[k] = v
		}
		for k, v := range servers {
			if _, ok := newConfig.Groups[k]; ok {
				return DuplicateGID
			}
			newConfig.Groups[k] = v
		}
		newConfig.Shards = MakeNewShards(lastConfig, newConfig.Groups)
	}
	sm.configs = append(sm.configs, newConfig)
	return OK
}

func (sm *ShardMaster) processLeave(gids []int) Err {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1}
	newConfig.Groups = make(map[int][]string)
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v
	}
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; !ok {
			return GIDNotExist
		}
		delete(newConfig.Groups, gid)
	}
	if len(newConfig.Groups) == 0 {
		newConfig.Shards = [NShards]int{}
	} else {
		newConfig.Shards = MakeNewShards(lastConfig, newConfig.Groups)
	}
	sm.configs = append(sm.configs, newConfig)
	return OK
}

// I don't know the purpose of move?
// I just need to swap the shard's previous owner with gid
func (sm *ShardMaster) processMove(shard int, gid int) Err {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1}
	newConfig.Groups = make(map[int][]string)
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v
	}
	if _, ok := newConfig.Groups[gid]; !ok {
		return GIDNotExist
	}
	if shard >= NShards || shard < 0 {
		return ShardNotExist
	}
	previousOwner := lastConfig.Shards[shard]
	first := true
	for oldShard, oldGid := range lastConfig.Shards {
		if oldGid == gid && first {
			newConfig.Shards[oldShard] = previousOwner
			first = false
		} else if oldShard == shard {
			newConfig.Shards[oldShard] = gid
		} else {
			newConfig.Shards[oldShard] = oldGid
		}
	}
	sm.configs = append(sm.configs, newConfig)
	return OK
}

func (sm *ShardMaster) processQuery(query int) (Err, Config) {
	if query >= len(sm.configs) || query < -1 {
		return ConfigNotExist, Config{}
	} else if query == -1 {
		return OK, sm.configs[len(sm.configs)-1]
	} else {
		return OK, sm.configs[query]
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// DPrintf("Receive join request")
	processedReply := sm.process(*args, args.Identifier, args.Counter)
	reply.BaseReply = processedReply.BaseReply
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	processedReply := sm.process(*args, args.Identifier, args.Counter)
	reply.BaseReply = processedReply.BaseReply
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	processedReply := sm.process(*args, args.Identifier, args.Counter)
	reply.BaseReply = processedReply.BaseReply
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	processedReply := sm.process(*args, args.Identifier, args.Counter)
	reply.BaseReply = processedReply.BaseReply
	reply.Config = processedReply.config
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	labgobInitialize()
	// Your code here.
	sm.lastRaftAppliedLogIndex = 0
	sm.opApplyCh = map[int]chan Reply{}
	sm.lastClerkAppliedOpIndex = map[int64]int{}
	go sm.checkApplyCh()

	return sm
}

func labgobInitialize() {
	labgob.Register(Op{})
	labgob.Register(BaseArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(MoveArgs{})
}
