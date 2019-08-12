package kvraft

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

const RetryInterval = time.Duration(150 * time.Millisecond)
type Err string
type Clerk struct {
		servers []*labrpc.ClientEnd
		clientID int64
		RequestSeq int
		leaderID	int
}
type PutAppendArgs struct {
	ClientId   int64
	RequestSeq int
	Key        string
	Value      string
	Op         string // (根据不同情况变为put 或者 append)
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{arg.ClientId, arg.RequestSeq, arg.Key, arg.Value, arg.Op}
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

func (arg *GetArgs) copy() GetArgs {
	return GetArgs{arg.Key}
}

type GetReply struct {
	Err   Err
	Value string
}
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) Call(rpcname string, args interface{}, reply interface{}) bool {
	return ck.servers[ck.leaderID].Call(rpcname, args, reply)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = nrand()
	ck.RequestSeq = 0
	ck.leaderID = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{key}
	for {
		var reply GetReply
		if ck.Call("KVServer.Get", &args, &reply) && reply.Err == OK {
			DPrintf("[%d GET key %s reply %#v]", ck.leaderID, args.Key, reply)
			return reply.Value
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.RequestSeq ++
	args := PutAppendArgs{ck.clientID, ck.RequestSeq, key, value, op}
	for {
		var reply PutAppendReply
		if ck.Call("KVServer.PutAppend", &args, &reply) && reply.Err == OK {
			if op == "Put" {
				DPrintf("[%d seq %d PUT key %s value %s reply %v]", ck.leaderID, args.RequestSeq, args.Key, args.Value, reply)
			} else {
				DPrintf("[%d seq %d APPEND key %s value %s reply %v]", ck.leaderID, args.RequestSeq, args.Key, args.Value, reply)
			}
			return
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}