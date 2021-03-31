package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaders            map[int]int
	identifier         int64
	lastAppliedOpIndex int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaders = map[int]int{}
	ck.identifier = nrand()
	ck.lastAppliedOpIndex = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.lastAppliedOpIndex++
	args := GetArgs{key, ck.identifier, ck.lastAppliedOpIndex}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.

			// let's try leader first
			if _, ok := ck.leaders[gid]; !ok {
				ck.leaders[gid] = 0
			}
			leader := ck.leaders[gid]
			srv := ck.make_end(servers[leader])
			reply := GetReply{}
			ok := srv.Call("ShardKV.Get", &args, &reply)
			if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			}
			if ok && (reply.Err == ErrWrongGroup) {
				goto query
			}

			// wrong leader, try all the servers in the group
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				reply := GetReply{}
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.leaders[gid] = si
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					goto query
				}
			}
		}
// we got wrong group, needs the latest config
query:
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.lastAppliedOpIndex++
	args := PutAppendArgs{key, value, op, ck.identifier, ck.lastAppliedOpIndex}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.leaders[gid]; !ok {
				ck.leaders[gid] = 0
			}
			leader := ck.leaders[gid]
			srv := ck.make_end(servers[leader])
			reply := PutAppendReply{}
			ok := srv.Call("ShardKV.PutAppend", &args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return 
			}
			if ok && (reply.Err == ErrWrongGroup) {
				goto query
			}

			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				reply := PutAppendReply{}
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					ck.leaders[gid] = si
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					goto query
				}
			}
		}
query:
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
