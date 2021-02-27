package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// ID int64
	Identifier int64
	Counter    int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// ID int64
	Identifier int64
	Counter    int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
