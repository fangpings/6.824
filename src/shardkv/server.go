package shardkv

// import "shardmaster"
import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"raft"
	"shardmaster"
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

const (
	normal = iota
	reconfigCollectData
	reconfigCollectAck
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string

	ClerkIdentifier int64
	ClearkOpIndex   int
	Err             Err
}

type NewConfig struct {
	Config shardmaster.Config
}

type ReconfigData struct {
	Data       map[string]string
	NextConfig shardmaster.Config
}

type ReconfigAck struct {
	Data       map[string]string
	NextConfig shardmaster.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister               *raft.Persister
	storage                 map[string]string
	opApplyCh               map[int]chan Op
	term                    int
	lastClerkAppliedOpIndex map[int64]int
	lastRaftAppliedLogIndex int
	mck                     *shardmaster.Clerk
	config                  shardmaster.Config
	state                   int // normal, reconfigStart, reconfigEnd
	nextConfig              shardmaster.Config
}

func (kv *ShardKV) checkApplyCh() {
	for msg := range kv.applyCh {
		raftLogAppliedIndex := msg.CommandIndex
		if command, ok := msg.Command.(raft.InstallSnapshotMsg); ok {
			// snapshot is not replicated through raft logs
			kv.installSnapshot(command.Data, false)
		} else {
			// for ops that come from raft logs
			// we need to ensure it's strictly sequenced
			if kv.lastRaftAppliedLogIndex != raftLogAppliedIndex-1 {
				kv.rf.SetLastApplied(kv.lastRaftAppliedLogIndex)
				continue
			} else {
				if kv.lastRaftAppliedLogIndex != raftLogAppliedIndex-1 {
					kv.rf.SetLastApplied(kv.lastRaftAppliedLogIndex)
					continue
				}
				op := Op{}
				switch command := msg.Command.(type) {
				case Op:
					DPrintf("Group %d ID %d Config %d received Op index %d", kv.gid, kv.me, kv.config.Num, raftLogAppliedIndex)
					clerkIdentifier := command.ClerkIdentifier
					clerkOpIndex := command.ClearkOpIndex
					// TODO: reject all the requests when we are in reconfig states
					kv.process(clerkIdentifier, clerkOpIndex, raftLogAppliedIndex, &command)
					op = command
				case NewConfig:
					// reconfiguration starts
					// this is not blocking because we might want keys
					// that are not affected to be processed in the mean time
					// also we want to make sure the config num and our current state matches
					DPrintf("Group %d ID %d Config %d received new config %d index %d", kv.gid, kv.me, kv.config.Num, command.Config.Num, raftLogAppliedIndex)
					kv.mu.Lock()
					// in the same config term we will allow states to advance more than 1 state
					// i.e. from data collect to end (skip ack collect)
					// because sometimes state changes might be lagging behind (since followers are just receiving logs from leaders)
					// but cross config terms advancement is not allowed
					// i.e. reconfig term 1 jump to term 3 (not sure whether it's correct or not?)
					// also state revert is not allowed, so we should never go back to previous state

					// state transistion must be done here, not in any other spawn goroutine
					// otherwise state transition might lag behind
					if kv.state > normal || command.Config.Num != kv.config.Num+1 {
						kv.mu.Unlock()
						break
					}
					kv.state = reconfigCollectData
					kv.mu.Unlock()

					go kv.reconfigDataCollect(command.Config)
				case ReconfigData:
					DPrintf("Group %d ID %d Config %d received reconfig data %d index %d", kv.gid, kv.me, kv.config.Num, command.NextConfig.Num, raftLogAppliedIndex)
					kv.mu.Lock()
					if kv.state > reconfigCollectData || command.NextConfig.Num != kv.config.Num+1 {
						kv.mu.Unlock()
						break
					}
					kv.state = reconfigCollectAck
					kv.mu.Unlock()

					go kv.reconfigAckCollect(command)
				case ReconfigAck:
					// this one needs to be blocking
					// because if we recover from failure and start replaying logs
					// we must prevent subsequent logs from executing because
					// we are not finished yet and those keys that are under reconfig
					// is not allowed to process
					DPrintf("Group %d ID %d Config %d received reconfig ack %d index %d", kv.gid, kv.me, kv.config.Num, command.NextConfig.Num, raftLogAppliedIndex)
					kv.reconfigEnd(command)
				}
				kv.lastRaftAppliedLogIndex = raftLogAppliedIndex
				// kv.persister.Log(raftLogAppliedIndex, command, kv.me, "", ret)
				// DPrintf("Server %d, OP %v applied to state machine", kv.me, command)
				if ch, ok := kv.opApplyCh[raftLogAppliedIndex]; ok {
					ch <- op
					delete(kv.opApplyCh, raftLogAppliedIndex)
				}
			}
		}
	}
}

func (kv *ShardKV) checkStateSize() {
	if kv.maxraftstate == -1 {
		// if maxraftstate is -1, we don't need this check
		return
	}
	for {
		if kv.persister.RaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.storage)
			e.Encode(kv.lastClerkAppliedOpIndex)
			e.Encode(kv.lastRaftAppliedLogIndex)
			e.Encode(kv.config)
			e.Encode(kv.state)
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

func (kv *ShardKV) checkConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader && kv.state == normal {
			kv.mu.Lock()
			next := kv.config.Num + 1
			kv.mu.Unlock()

			config := kv.mck.Query(next)

			// config.Num will be 0 if next does not exist
			// in this way we can update the config one by one
			if config.Num > kv.config.Num {
				DPrintf("Group %d ID %d Config %d new config detected %d, new config %v, current config %v", kv.gid, kv.me, kv.config.Num, config.Num, config, kv.config)
				// we need to update the config
				// once we submit the new config to raft
				// we should wait until it is replicated
				// otherwise we might send too many replicates or miss something
				kv.rf.Start(NewConfig{config})
				// index, _, isLeader := kv.rf.Start(NewConfig{config})
				// if isLeader {
				// 	listenCh := make(chan Op, 1)
				// 	kv.mu.Lock()
				// 	kv.opApplyCh[index] = listenCh
				// 	kv.mu.Unlock()

				// 	select {
				// 	case <-time.After(time.Duration(100 * time.Millisecond)):
				// 		kv.mu.Lock()
				// 		delete(kv.opApplyCh, index)
				// 		kv.mu.Unlock()
				// 	case <-listenCh:
				// 		break
				// 	}
				// }
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) installSnapshot(data []byte, restart bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
	var config shardmaster.Config
	var state int
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
	if e := d.Decode(&config); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	if e := d.Decode(&state); e != nil {
		DPrintf("FATAL DECODE ERROR %s", e)
		return
	}
	// if e := d.Decode(&appliedOp); e != nil {
	// 	DPrintf("FATAL DECODE ERROR %s", e)
	// 	return
	// }

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
	kv.config = config
	kv.state = state
	if restart && state != normal {
		go func() { 
			nextConfig := kv.mck.Query(config.Num+1)
			kv.mu.Lock()
			if nextConfig.Num > kv.config.Num {
				kv.state = reconfigCollectData
				go kv.reconfigDataCollect(nextConfig)
			} else {
				DPrintf("WHY THIS WOULD HAPPEN")
			}
			kv.mu.Unlock()
		}()
	}
	kv.rf.SetLastApplied(kv.lastRaftAppliedLogIndex)
}

// process one get/put/append command
func (kv *ShardKV) process(clerkIdentifier int64, clerkOpIndex int, raftLogAppliedIndex int, command *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.state != normal {
		command.Err = ErrWrongGroup
		DPrintf("Group %d ID %d Config %d op %d omitted because in reconfig %v", kv.gid, kv.me, kv.config.Num, raftLogAppliedIndex, command)
		return
	}
	if _, ok := kv.lastClerkAppliedOpIndex[clerkIdentifier]; !ok {
		kv.lastClerkAppliedOpIndex[clerkIdentifier] = clerkOpIndex - 1
	}
	new := clerkOpIndex > kv.lastClerkAppliedOpIndex[clerkIdentifier]
	DPrintf("Group %d ID %d Config %d op %d processing %v", kv.gid, kv.me, kv.config.Num, raftLogAppliedIndex, command)
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
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.state != normal || kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	op := Op{"Get", args.Key, "", args.Identifier, args.Counter, ""}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	listenCh := make(chan Op, 1)
	kv.opApplyCh[index] = listenCh
	kv.mu.Unlock()

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
			reply.Value = appliedOp.Value
			if appliedOp.Err == ErrNoKey || appliedOp.Err == ErrWrongGroup {
				reply.Err = appliedOp.Err
			} else {
				reply.Err = OK
			}
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.state != normal || kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("Group %d ID %d Config %d Wrong group", kv.gid, kv.me, kv.config.Num)
		return
	}
	if _, ok := kv.lastClerkAppliedOpIndex[args.Identifier]; ok && args.Counter < kv.lastClerkAppliedOpIndex[args.Identifier] {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	op := Op{args.Op, args.Key, args.Value, args.Identifier, args.Counter, ""}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	listenCh := make(chan Op, 1)
	kv.opApplyCh[index] = listenCh
	kv.mu.Unlock()

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
			if appliedOp.Err == ErrWrongGroup {
				reply.Err = appliedOp.Err
			} else {
				reply.Err = OK
			}
		}
	}
}

// will return 1 if that sever group is in right stage
// otherwise keeping retrying until we get what we want
// or return 0 if we find out that the leader changed
//
// Note, we cannot use kv.config here because we might silently enter next reconfig term without knowing anything
func (kv *ShardKV) sendGetState(gid int, newConfig shardmaster.Config, okChan chan int) {
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); kv.state != reconfigCollectAck || !isLeader {
			kv.mu.Unlock()
			okChan <- 0
			return
		}
		args := GetStateArgs{}
		reply := GetStateReply{}
		group := newConfig.Groups[gid]
		// each time we randomly pick one server in the group
		// in case a specific server in the group is disconnected
		srv := kv.make_end(group[rand.Intn(len(group))])
		kv.mu.Unlock()
		ok := srv.Call("ShardKV.GetState", &args, &reply)
		if ok && (reply.ConfigNum >= newConfig.Num || (reply.ConfigNum+1 == newConfig.Num && reply.State == reconfigCollectAck)) {
			// they are at least in the same reconfig stage (reconfigEnd) as we are
			// so they must get our data
			// we can safely return ack
			okChan <- 1
			return
		}
		// they have not come to our reconfig stage, we have to wait
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) sendGetShards(gid int, shards []int, config shardmaster.Config, resChan chan map[string]string) {
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); kv.state != reconfigCollectData || !isLeader {
			kv.mu.Unlock()
			resChan <- nil
			return
		}
		args := GetShardArgs{config.Num, shards}
		reply := GetShardReply{}
		group := config.Groups[gid]
		srv := kv.make_end(group[rand.Intn(len(group))])
		kv.mu.Unlock()
		ok := srv.Call("ShardKV.GetShards", &args, &reply)
		if ok {
			if reply.ConfigNum > config.Num {
				// they left this reconfig term, we should not retry
				resChan <- nil
				return
			} else if reply.ConfigNum == config.Num && reply.State != normal {
				// they are in the same reconfig term as we are
				resChan <- reply.Data
				DPrintf("Group %d ID %d Config %d sent get shards %v got %v", kv.gid, kv.me, kv.config.Num, shards, reply.Data)
				return
			}
		}
		// they have not come to the reconfig stage where we are
		time.Sleep(50 * time.Millisecond)
	}
}

// servers we need to get ack from (severs that needs to get data from us)
func (kv *ShardKV) getAckServers(oldconfig shardmaster.Config, newconfig shardmaster.Config) []int {
	set := map[int]struct{}{}
	for i := 0; i < shardmaster.NShards; i++ {
		oldGid := oldconfig.Shards[i]
		newGid := newconfig.Shards[i]
		if oldGid == kv.gid && newGid != kv.gid {
			set[newGid] = struct{}{}
		}
	}
	ret := []int{}
	for key := range set {
		ret = append(ret, key)
	}
	return ret
}

// servers we need to get data from
func (kv *ShardKV) getPullFromServers(oldconfig shardmaster.Config, newconfig shardmaster.Config) map[int][]int {
	ret := map[int][]int{}
	for i := 0; i < shardmaster.NShards; i++ {
		oldGid := oldconfig.Shards[i]
		newGid := newconfig.Shards[i]
		if oldGid != 0 && oldGid != kv.gid && newGid == kv.gid {
			if _, ok := ret[oldGid]; !ok {
				ret[oldGid] = []int{i}
			} else {
				ret[oldGid] = append(ret[oldGid], i)
			}
		}
	}
	return ret
}

// all sever will call this function, but only those who think they are leader
// will actually collect data from others.
// the rest will just sleep and wake up to see whether it is still in reconfigStart state
// and it has become the leader. if so, it will start collecting data
func (kv *ShardKV) reconfigDataCollect(nextConfig shardmaster.Config) {
	kv.mu.Lock()
	config := kv.config
	kv.mu.Unlock()
	DPrintf("Group %d ID %d Config %d reconfig start collecting data %d", kv.gid, kv.me, kv.config.Num, nextConfig.Num)
	for {
		kv.mu.Lock()
		if kv.state != reconfigCollectData || kv.config.Num != config.Num {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			candidates := kv.getPullFromServers(config, nextConfig)
			DPrintf("Group %d ID %d Config %d getting reconfig data from %v", kv.gid, kv.me, kv.config.Num, candidates)
			resChan := make(chan map[string]string)
			for gid, shards := range candidates {
				go kv.sendGetShards(gid, shards, config, resChan)
			}
			expected := len(candidates)
			allData := map[string]string{}
			get := 0
			kv.mu.Unlock()
			if expected > 0 {
				func() {
					for {
						select {
						case data := <-resChan:
							// chances are that we are lagging behind
							// in that case retry is meaningless
							if data == nil {
								return
							}
							for k, v := range data {
								allData[k] = v
							}
							get++
							if get == expected {
								return
							}
						}
					}
				}()
			}
			if get == expected {
				// we get all the data we need
				// replicate what we get through raft
				reconfigData := ReconfigData{allData, nextConfig}
				index, _, isLeader := kv.rf.Start(reconfigData)
				if isLeader {
					listenCh := make(chan Op, 1)
					kv.mu.Lock()
					kv.opApplyCh[index] = listenCh
					kv.mu.Unlock()

					select {
					case <-time.After(time.Duration(1000 * time.Millisecond)):
						kv.mu.Lock()
						delete(kv.opApplyCh, index)
						kv.mu.Unlock()
					case <-listenCh:
						return
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) reconfigAckCollect(reconfigData ReconfigData) {
	kv.mu.Lock()
	config := kv.config
	kv.mu.Unlock()
	DPrintf("Group %d ID %d Config %d reconfig start collecting ack %d", kv.gid, kv.me, kv.config.Num, reconfigData.NextConfig.Num)
	for {
		kv.mu.Lock()
		if kv.state != reconfigCollectAck || kv.config.Num != config.Num {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			candidates := kv.getAckServers(config, reconfigData.NextConfig)
			ackChan := make(chan int)
			for _, gid := range candidates {
				go kv.sendGetState(gid, reconfigData.NextConfig, ackChan)
			}
			kv.mu.Unlock()
			expected := len(candidates)
			get := 0
			if expected > 0 {
				func() {
					for {
						select {
						case ack := <-ackChan:
							if ack == 0 {
								return
							}
							get++
							if get == expected {
								return
							}
						}
					}
				}()
			}

			if get == expected {
				ack := ReconfigAck{reconfigData.Data, reconfigData.NextConfig}
				index, _, isLeader := kv.rf.Start(ack)
				if isLeader {
					listenCh := make(chan Op, 1)
					kv.mu.Lock()
					kv.opApplyCh[index] = listenCh
					kv.mu.Unlock()

					select {
					case <-time.After(time.Duration(1000 * time.Millisecond)):
						kv.mu.Lock()
						delete(kv.opApplyCh, index)
						kv.mu.Unlock()
					case <-listenCh:
						return
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) reconfigEnd(reconfigAck ReconfigAck) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// we are allowed to jump form normal/data collect/ack collect to next normal
	// as long as the term matches
	if reconfigAck.NextConfig.Num != kv.config.Num+1 {
		DPrintf("Group %d ID %d Config %d state %d next config num %d", kv.gid, kv.me, kv.config.Num, kv.state, reconfigAck.NextConfig.Num)
		return
	}
	DPrintf("Group %d ID %d Config %d reconfig end %d", kv.gid, kv.me, kv.config.Num, reconfigAck.NextConfig.Num)
	kv.state = normal
	kv.config = reconfigAck.NextConfig
	DPrintf("Group %d ID %d Config %d moving data %v to storage, my storage %v", kv.gid, kv.me, kv.config.Num, reconfigAck.Data, kv.storage)
	for k, v := range reconfigAck.Data {
		// TODO: delete keys here
		kv.storage[k] = v
	}
}

// GetState 是一个仅在server之间使用的rpc
// 他的作用是让别的server知道本服务器组的config num和状态
// 注意这个rpc会由收到请求的server直接返回而不经过raft
// 这么做的理由是如果经过raft会让log变得非常混乱
// 并且关于状态，收到请求的服务器如果暂时没有更新也不要紧，发起请求的服务器稍后会重试
// 而已经更新的状态并不会消失而是会永远存在
// 例如当前多数状态可能在 config: 4, reconfig_end
// 但是当前服务器因为raft落后而仍处于config 3 normal
// 那么我们询问的服务器(假设正在进行reconfig 4)
// 会认为当前服务器还未进行reconfig4
// 那么它会过一段时间再来询问，此时当前服务器已经成功收到raft log并进入config: 4, reconfig_end
// 此时询问服务器就会收到正确的状态
// 并且我们无需担心重启的问题，GetStart是在新的goroutine中进行的，并不会block下一个log
// 并且即使出现network partition也没事，在每次发起询问时，
// 发起询问的服务器会随机选择本服务器组的任意服务器
func (kv *ShardKV) GetState(args *GetStateArgs, reply *GetStateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.ConfigNum = kv.config.Num
	reply.State = kv.state
	return
}

// GetShards是在server之间实际传送数据的函数
// 这个函数只有在同一个reconfig中才会转移数据，并且不会在normal状态下转移数据
// 因为只有非normal状态我们才会停止接受新的请求
// GetShards函数也不会在raft中同步
// 理由是在reconfig阶段我们不接受新的请求，故storage是保持不变的
func (kv *ShardKV) GetShards(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.ConfigNum = kv.config.Num
	reply.State = kv.state
	if args.ConfigNum == kv.config.Num && kv.state != normal {
		data := map[string]string{}
		for k, v := range kv.storage {
			for _, i := range args.Shards {
				if key2shard(k) == i {
					data[k] = v
				}
			}
		}
		reply.Data = data
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(NewConfig{})
	labgob.Register(ReconfigAck{})
	labgob.Register(ReconfigData{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.storage = map[string]string{}
	kv.opApplyCh = map[int]chan Op{}
	kv.lastClerkAppliedOpIndex = map[int64]int{}
	kv.lastRaftAppliedLogIndex = 0
	kv.term = 0
	kv.config = shardmaster.Config{}
	kv.state = normal

	kv.installSnapshot(kv.persister.ReadSnapshot(), true)
	go kv.checkApplyCh()
	go kv.checkStateSize()
	go kv.checkConfig()

	return kv
}
