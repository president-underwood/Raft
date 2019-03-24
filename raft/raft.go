package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "math/rand"
import "time"
import "fmt"
import "bytes"
import "labgob"

const(
	Follower= iota
	Candidate
	Leader

)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyChannel; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot     []byte
}

type entries struct {
	Command interface{}
	Term    int
	Index   int
}

//
// A Go object implementing a single Raft peer（实现一个raft端)
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state(可持久保持的状态)
	currentTerm int
	votedFor    int
	log         []entries
	//volatile state(可变状态)
	state       int
	commitIndex int
	lastApplied int

	//leaders(领导者的)
	nextIndex  []int
	matchIndex []int

	//channel
	chanAppendEntries chan int
	chanvoteGranted   chan int
	chanLeader        chan int
	chanApplyMsg      chan ApplyMsg
	chanCommit        chan int
}

// return currentTerm and whether this server
// believes it is the leader.
//获取当前轮数和peer's State
func (r *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = r.currentTerm
	if r.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}
//获取日志数组的长度
func (r*Raft)GetLen()(int){
	return len(r.log)-1
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (r *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w:=new(bytes.Buffer)//create a bytes,Buffer which made by []byts to io.Writer
	e :=labgob.NewEncoder(w)
	e.Encode(r.currentTerm)//1.persistment state
	e.Encode(r.votedFor)//2.persistment state
	e.Encode(r.log)//3.persistment state
	data:=w.Bytes()
	r.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curt int
	var vote int
	var Llog []entries
	if d.Decode(&curt) != nil || d.Decode(&vote) != nil || d.Decode(&Llog) != nil {
		fmt.Println("server ", rf.me, " readPersist wrong!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = curt
		rf.ClearChange()
		rf.votedFor = vote
		rf.log =Llog
		rf.mu.Unlock()
	}
}

//对照lab
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//(RPC服务 RequestVote 元素)
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//根据论文定义结构体
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entries

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	PrevIndex int
	Success   bool
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}

func (rf *Raft) ClearChange() {
	rf.chanvoteGranted = make(chan int, 10000)
	rf.chanAppendEntries = make(chan int, 10000)
}
//leader election
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.persist()
	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.ClearChange()
		r.state = Follower
		r.votedFor = -1

	}
	//除了这两种情况，其他的情况(包括args.Term=r.currentTerm but r.state==candidate,r.Term<args,Term change the r.state into Follower then judge whether)
	reply.Term = r.currentTerm
	reply.VoteGranted = false
	if args.Term >= r.currentTerm {
		if r.votedFor == -1 || r.votedFor == args.CandidateId {
			if args.LastLogTerm > r.log[r.GetLen()].Term || (args.LastLogTerm == r.log[r.GetLen()].Term && args.LastLogIndex >= r.log[r.GetLen()].Index) {
				r.chanvoteGranted <- 1
				reply.VoteGranted = true
				r.state = Follower
				r.votedFor = args.CandidateId
				//fmt.Println(rf.me, "voted for ", rf.votedFor)
			}
		}
	}
}
//日志复制，添加日志项目
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//have no entries
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ClearChange()
		rf.votedFor = -1
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.PrevIndex = args.PrevLogIndex
	if args.Term >= rf.currentTerm {
		rf.chanAppendEntries <- 1
		FirstIndex := rf.log[0].Index
		NowIndex := args.PrevLogIndex - FirstIndex
		if NowIndex < 0 {
			reply.PrevIndex = FirstIndex
			return
		}
		if rf.GetLen() < NowIndex || rf.log[NowIndex].Term != args.PrevLogTerm {
			reply.Success = false
			if rf.GetLen() >= NowIndex {
				tmp := NowIndex
				for rf.log[tmp].Term == rf.log[NowIndex].Term && tmp > 0 {
					tmp--
				}
				reply.PrevIndex = rf.log[tmp].Index
			} else {
				reply.PrevIndex = rf.log[rf.GetLen()].Index
			}
		} else {
			reply.Success = true
			if rf.GetLen() >= NowIndex+1 {
				rf.log = rf.log[:NowIndex+1]
			}
			rf.log = append(rf.log, args.Entries...) //append a slice
			// if len(args.Entries) > 0{
			//  fmt.Println(rf.log)
			// }
			if rf.commitIndex < min(args.LeaderCommit, rf.log[rf.GetLen()].Index) { //rules for followers to increase commitIndex
				rf.commitIndex = min(args.LeaderCommit, rf.log[rf.GetLen()].Index)
				rf.chanCommit <- 1
				//fmt.Println("commitIndex: ", rf.me, rf.commitIndex)
			}
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.ClearChange()
		rf.votedFor = -1
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	rf.chanAppendEntries <- 1
	FirstIndex := rf.log[0].Index
	NowIndex := args.LastIncludeIndex - FirstIndex
	if NowIndex < 0 {
		rf.mu.Unlock()
		return
	}
	if rf.GetLen() >= NowIndex && rf.log[NowIndex].Term == args.LastIncludeTerm {
		rf.log = rf.log[NowIndex:]
	} else {
		rf.log = make([]entries, 0)
		rf.log = append(rf.log, entries{0, args.LastIncludeTerm, args.LastIncludeIndex})
	}
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Snapshot)
	msg := ApplyMsg{CommandValid: false, Snapshot: args.Snapshot}
	rf.lastApplied = args.LastIncludeIndex
	rf.commitIndex = rf.lastApplied
	rf.mu.Unlock()
	rf.chanApplyMsg <- msg
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble gettCounting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader = rf.GetState()
	if isLeader {
		index = rf.log[rf.GetLen()].Index + 1
		rf.log = append(rf.log, entries{command, term, index})
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}
//handle RequestVote
func (rf *Raft) handleRequestVote() {
	//fmt.Println(rf.me, "becomes candidate, term is", rf.currentTerm)
	count := 1
	rf.mu.Lock()
	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.log[rf.GetLen()].Index, rf.log[rf.GetLen()].Term}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == Candidate { //must candidate can send requestvote
			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok == true && rf.state == Candidate {
					if args.Term != rf.currentTerm { //candidate timeout and start a new election
						return
					}
					if reply.Term > rf.currentTerm { //candidate's term is out of date
						rf.state = Follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.ClearChange()
						rf.persist()
						return
					}
					if reply.VoteGranted == true {
						count += 1
						if count == len(rf.peers)/2+1 {
							if rf.state == Candidate { //maybe rf hasn't been a candidate because of receiving leader's rpc
								//fmt.Println(rf.me, "become leader")
								rf.chanLeader <- 1
							}
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) allAppendEntries() {
	//rules for leaders to increase the commitIndex
	rf.mu.Lock()
	defer rf.mu.Unlock()
	N := rf.commitIndex
	FirstIndex := rf.log[0].Index
	for i := max(rf.commitIndex+1, FirstIndex+1); i <= rf.log[rf.GetLen()].Index; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me {
				if rf.matchIndex[j] >= i {
					if rf.log[i-FirstIndex].Term == rf.currentTerm {
						num++
					}
				}
			}
		}
		if num > len(rf.peers)/2 {
			N = i
		}
	}
	if N > rf.commitIndex && rf.state == Leader {
		rf.commitIndex = min(N, rf.log[rf.GetLen()].Index)
		rf.chanCommit <- 1
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == Leader {
			if rf.nextIndex[i] > FirstIndex {
				etr := make([]entries, 0)
				//fmt.Println(i, etr)
				tmpIndex := min(max(0, rf.nextIndex[i]-1), rf.log[rf.GetLen()].Index)
				if tmpIndex < rf.log[rf.GetLen()].Index {
					etr = rf.log[tmpIndex+1-FirstIndex:]
				}
				args := &AppendEntriesArgs{rf.currentTerm, rf.me, tmpIndex, rf.log[tmpIndex-FirstIndex].Term, etr, rf.commitIndex}
				go func(args *AppendEntriesArgs, i int) {
					reply := &AppendEntriesReply{}
					//fmt.Println(rf.me)
					//fmt.Println(rf.me, i)
					ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if ok == true && rf.state == Leader {
						if args.Term != rf.currentTerm {
							return
						}
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							rf.ClearChange()
							rf.persist()
							return
						}
						if reply.Success == true && rf.state == Leader {
							if len(args.Entries) > 0 {
								rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
								rf.matchIndex[i] = rf.nextIndex[i] - 1
							}
						} else if rf.state == Leader {
							if rf.nextIndex[i] > reply.PrevIndex+1 {
								rf.nextIndex[i] = reply.PrevIndex + 1
							} else {
								rf.nextIndex[i] = max(rf.nextIndex[i]-1, 1)
							}
						}
					}
				}(args, i)
			} else {
				//snapshot
				//fmt.Println(rf.me, "send snapshot to ", i)
				args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.log[0].Index, rf.log[0].Term, rf.persister.ReadSnapshot()}
				go func(args *InstallSnapshotArgs, i int) {
					reply := &InstallSnapshotReply{}
					ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if ok == true && rf.state == Leader {
						if args.Term != rf.currentTerm {
							return
						}
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							rf.ClearChange()
							rf.persist()
							return
						}
						rf.nextIndex[i] = args.LastIncludeIndex + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
					}
				}(args, i)
			}
		}
	}
}

func (rf *Raft) SaveSnapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FirstIndex := rf.log[0].Index
	if index < FirstIndex {
		return
	}
	rf.log = rf.log[index-FirstIndex:]
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), data)
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rf *Raft) doStateChange() {
	for {
		rf.mu.Lock()
		st := rf.state
		rf.mu.Unlock()
		switch st {
		case Follower:
			select {
			case <-rf.chanvoteGranted: //receive requestVote rpc
				rf.mu.Lock()
				rf.ClearChange()
				rf.mu.Unlock()
			case <-rf.chanAppendEntries: //receive AppendEntries rpc
				rf.mu.Lock()
				rf.ClearChange()
				rf.mu.Unlock()
			case <-time.After(time.Millisecond * time.Duration(750+rand.Int31n(500))): //election timeout
				rf.mu.Lock()
				if len(rf.chanvoteGranted) == 0 && len(rf.chanAppendEntries) == 0 {
					rf.state = Candidate
					rf.currentTerm += 1 //every election start term += 1
					rf.votedFor = rf.me
					rf.persist()
				}
				rf.ClearChange()
				rf.mu.Unlock()
			}
		case Candidate:
			go rf.handleRequestVote() //send rpc
			select {
			case <-time.After(time.Millisecond * time.Duration(750+rand.Int31n(500))): //election timeout and start a new election
				rf.mu.Lock()
				if len(rf.chanvoteGranted) == 0 && len(rf.chanAppendEntries) == 0 {
					rf.state = Candidate
					rf.currentTerm += 1 //every election start term += 1
					rf.votedFor = rf.me
					rf.persist()
				}
				rf.ClearChange()
				rf.mu.Unlock()
			case <-rf.chanAppendEntries: // receive leader rpc and convert to follower
				rf.mu.Lock()
				rf.state = Follower
				rf.ClearChange()
				rf.mu.Unlock()
			case <-rf.chanLeader: // receive more than majority vote
				rf.mu.Lock()
				rf.state = Leader
				//fmt.Println(rf.me, "become leader, term is", rf.currentTerm)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.log[rf.GetLen()].Index + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			}
		case Leader:
			go rf.allAppendEntries()
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}
}

func (rf *Raft) doApply() {
	for {
		select {
		case <-rf.chanCommit:
			//fmt.Println("raft apply:", rf.me, rf.lastApplied)
			for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.log[rf.GetLen()].Index {
				rf.mu.Lock()
				if rf.lastApplied >= rf.commitIndex {
					break
				}
				FirstIndex := rf.log[0].Index
				if rf.lastApplied+1 >= FirstIndex {
					msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied+1-FirstIndex].Command, CommandIndex: rf.lastApplied + 1}
					rf.mu.Unlock()
					//fmt.Println(rf.me, "want to apply", rf.lastApplied+1)
					//can't lock when send in channel, dead lock
					rf.chanApplyMsg <- msg
					rf.mu.Lock()
				}
				rf.lastApplied++
				rf.mu.Unlock()
				//fmt.Println(rf.me, "lastApplied", rf.lastApplied, rf.commitIndex)
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]entries, 0)
	rf.log = append(rf.log, entries{0, 0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanvoteGranted = make(chan int, 10000)
	rf.chanAppendEntries = make(chan int, 10000)
	rf.chanLeader = make(chan int, 10000)
	rf.chanApplyMsg = applyCh
	rf.chanCommit = make(chan int, 10000)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.doStateChange()

	go rf.doApply()

	return rf
}
