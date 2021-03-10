package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader             int
	identifier         int64
	lastAppliedOpIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leader = 0
	ck.identifier = nrand()
	ck.lastAppliedOpIndex = -1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.lastAppliedOpIndex++
	baseArgs := BaseArgs{ck.identifier, ck.lastAppliedOpIndex}
	args := QueryArgs{num, baseArgs}
	reply := QueryReply{}

	ok := ck.servers[ck.leader].Call("ShardMaster.Query", &args, &reply)
	if ok && !reply.WrongLeader {
		return reply.Config
	}
	
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := QueryReply{}
			ok := ck.servers[i].Call("ShardMaster.Query", &args, &reply)
			if ok && !reply.WrongLeader { 
				ck.leader = i
				return reply.Config
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.lastAppliedOpIndex++
	baseArgs := BaseArgs{ck.identifier, ck.lastAppliedOpIndex}
	args := JoinArgs{servers, baseArgs}
	reply := JoinReply{}

	ok := ck.servers[ck.leader].Call("ShardMaster.Join", &args, &reply)
	if ok && !reply.WrongLeader {
		return
	}
	
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := JoinReply{}
			ok := ck.servers[i].Call("ShardMaster.Join", &args, &reply)
			if ok && !reply.WrongLeader { 
				ck.leader = i
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.lastAppliedOpIndex++
	baseArgs := BaseArgs{ck.identifier, ck.lastAppliedOpIndex}
	args := LeaveArgs{gids, baseArgs}
	reply := LeaveReply{}

	ok := ck.servers[ck.leader].Call("ShardMaster.Leave", &args, &reply)
	if ok && !reply.WrongLeader {
		return
	}
	
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := LeaveReply{}
			ok := ck.servers[i].Call("ShardMaster.Leave", &args, &reply)
			if ok && !reply.WrongLeader { 
				ck.leader = i
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.lastAppliedOpIndex++
	baseArgs := BaseArgs{ck.identifier, ck.lastAppliedOpIndex}
	args := MoveArgs{shard, gid, baseArgs}
	reply := MoveReply{}

	ok := ck.servers[ck.leader].Call("ShardMaster.Move", &args, &reply)
	if ok && !reply.WrongLeader {
		return
	}
	
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := MoveReply{}
			ok := ck.servers[i].Call("ShardMaster.Move", &args, &reply)
			if ok && !reply.WrongLeader { 
				ck.leader = i
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}
