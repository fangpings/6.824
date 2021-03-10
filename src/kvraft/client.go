package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.leader = 0
	ck.identifier = nrand()
	ck.lastAppliedOpIndex = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.lastAppliedOpIndex++
	args := GetArgs{key, ck.identifier, ck.lastAppliedOpIndex}
	reply := GetReply{}

	ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
	if ok && !reply.WrongLeader {
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			args := GetArgs{key, ck.identifier, ck.lastAppliedOpIndex}
			reply := GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok && !reply.WrongLeader { 
				ck.leader = i
				if reply.Err == ErrNoKey {
					return ""
				}
				return reply.Value
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.lastAppliedOpIndex++
	args := PutAppendArgs{key, value, op, ck.identifier, ck.lastAppliedOpIndex}
	reply := PutAppendReply{}

	ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
	if ok && !reply.WrongLeader {
		return
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			args := PutAppendArgs{key, value, op, ck.identifier, ck.lastAppliedOpIndex}
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
