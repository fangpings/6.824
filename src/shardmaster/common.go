package shardmaster

import "sort"
// import "log"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

func OptimalShards(ngroups int) []int {
	min := NShards / ngroups
	residue := NShards % ngroups
	ret := make([]int, ngroups)
	for i := 0; i < ngroups; i++ {
		ret[i] = min
	}
	for i := 0; i < residue; i++ {
		ret[i]++
	}
	return ret
}

func MakeNewShards(oldConfig Config, newGroups map[int][]string) [NShards]int {
	oldDistribution := oldConfig.GetDistribution()
	// it turns out that we can have groups more than NShards so we must take care of this
	// if len(newGroups) > NShards then we need to pick everything that exists in old config
	// and fill the vacancy with what we got
	tmpGroups := map[int][]string{}
	if len(newGroups) > NShards {
		// keep everything that is in both old and new config
		for _, pair := range oldDistribution {
			if _, ok := newGroups[pair.Key]; ok {
				tmpGroups[pair.Key] = newGroups[pair.Key]
			}
		}
		i := len(tmpGroups) + 1
		// randomly select some to fill the vacancy
		for k, v := range newGroups {
			if _, ok := tmpGroups[k]; !ok {
				if i > NShards {
					break
				}
				tmpGroups[k] = v
				i++
			}
		}
		newGroups = tmpGroups
	}
	optimalShards := OptimalShards(len(newGroups))
	// 2 passes
	// 1st pass: grab all the releasing resources, i.e. groups that will holds less shard after change
	// 2st pass: assign available resources to groups that needs more resources
	resources := make([]int, 0)
	newCursor := 0
	newDistribution := make([]Pair, 0)
	for _, pair := range oldDistribution {
		if _, ok := newGroups[pair.Key]; !ok {
			// removed in new config, release all resources, don't move cursor
			resources = append(resources, pair.Value...)
			pair.Value = make([]int, 0)
		} else {
			// release extra resources
			if optimalShards[newCursor] < len(pair.Value) {
				difference := len(pair.Value) - optimalShards[newCursor]
				resources = append(resources, pair.Value[:difference]...)
				pair.Value = pair.Value[difference:]
			}
			newCursor++
			newDistribution = append(newDistribution, pair)
		}
	}
	addedGroups := map[int]int{}
	for _, pair := range newDistribution {
		addedGroups[pair.Key] = 0
	}
	for k := range newGroups {
		if _, ok := addedGroups[k]; !ok {
			newDistribution = append(newDistribution, Pair{k, []int{}})
		}
	}
	newShards := [NShards]int{}
	for i, pair := range newDistribution {
		if len(pair.Value) < optimalShards[i] {
			difference := optimalShards[i] - len(pair.Value)
			pair.Value = append(pair.Value, resources[:difference]...)
			resources = resources[difference:]
		}
		for _, shard := range pair.Value {
			newShards[shard] = pair.Key
		}
	}
	return newShards
}

type Pair struct {
	Key   int
	Value []int
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cfg *Config) GetDistribution() []Pair {
	distributionMap := make(map[int][]int)
	for i := 0; i < NShards; i++ {
		distributionMap[cfg.Shards[i]] = append(distributionMap[cfg.Shards[i]], i)
	}
	distribution := make([]Pair, 0)
	for k, v := range distributionMap {
		distribution = append(distribution, Pair{k, v})
	}
	sort.Slice(distribution, func(i, j int) bool {
		return len(distribution[i].Value) > len(distribution[j].Value)
	})
	return distribution
}

const (
	OK             = "OK"
	TooManyGroups  = "TOO_MANY_GROUPS"
	DuplicateGID   = "DUPLICATE_GID"
	GIDNotExist    = "GID_NOT_EXIST"
	ShardNotExist  = "SHARD_NOT_EXIST"
	ConfigNotExist = "CONFIG_NOT_EXIST"
)

type Err string

type BaseArgs struct {
	Identifier int64
	Counter    int
}

type BaseReply struct {
	WrongLeader bool
	Err         Err
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	BaseArgs
}

type JoinReply struct {
	BaseReply
}

type LeaveArgs struct {
	GIDs []int
	BaseArgs
}

type LeaveReply struct {
	BaseReply
}

type MoveArgs struct {
	Shard int
	GID   int
	BaseArgs
}

type MoveReply struct {
	BaseReply
}

type QueryArgs struct {
	Num int // desired config number
	BaseArgs
}

type QueryReply struct {
	Config Config
	BaseReply
}
