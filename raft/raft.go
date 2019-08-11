package raft

import (
	"bytes"
	crand "crypto/rand"
	"labgob"
	"labrpc"
	"log"
	"math/big"
	"math/rand"
	"sync"
	"time"
)

const AppendEntriesInterval = time.Duration(100 * time.Millisecond)
const ElectionTimeout = time.Duration(1000 * time.Millisecond)


func init() {
	labgob.Register(LogEntry{})
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	seed := bigx.Int64()
	rand.Seed(seed)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}


func newRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return time.Duration(minDuration + extra)
}

// 实现单个raft
type Raft struct {
	mu                sync.Mutex
	peers             []*labrpc.ClientEnd
	persister         *Persister          // 持久化化节点状态
	me                int
	leaderId          int
	currentTerm       int
	votedFor          int
	commitIndex       int
	lastApplied       int
	lastIncludedIndex int
	logIndex          int
	state             serverState
	shutdown          chan struct{}
	log               []LogEntry
	nextIndex         []int
	matchIndex        []int
	applyCh           chan ApplyMsg
	notifyApplyCh     chan struct{}
	electionTimer     *time.Timer         // 开始竞选新的leader
}

func (rf *Raft) resetElectionTimer(duration time.Duration) {

	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
}


func (rf *Raft) initIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}


func (rf *Raft) persist() {
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.logIndex)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

// 恢复当前持久化的状态
func (rf *Raft) readPersistState() {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&rf.log) != nil {
		log.Fatal("Error in unmarshal raft state")
	}
	rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.logIndex, rf.commitIndex, rf.lastApplied = currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied
}

func (rf *Raft) PersistAndSaveSnapshot(lastIncludedIndex int, snapshot [] byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex > rf.lastIncludedIndex {
		truncationStartIndex := rf.getOffsetIndex(lastIncludedIndex)
		rf.log = append([]LogEntry{}, rf.log[truncationStartIndex:]...)
		rf.lastIncludedIndex = lastIncludedIndex
		data := rf.getPersistState()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
}


func (rf *Raft) getOffsetIndex(i int) int {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) getEntry(i int) LogEntry {
	offsetIndex := rf.getOffsetIndex(i)
	return rf.log[offsetIndex]
}

func (rf *Raft) getRangeEntry(fromInclusive, toExclusive int) []LogEntry {
	from := rf.getOffsetIndex(fromInclusive)
	to := rf.getOffsetIndex(toExclusive)
	return append([]LogEntry{}, rf.log[from:to]...)
}


func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor, rf.leaderId = -1, -1
	rf.persist()
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
}

func (rf *Raft) notifyNewLeader() {
	rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: -1, CommandTerm: -1, Command: "NewLeader"}
}


func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.getEntry(index).LogTerm == rf.currentTerm {
		majority, count := len(rf.peers)/2+1, 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= index {
				count += 1
			}
		}
		return count >= majority
	} else {
		return false
	}
}


func (rf *Raft) solicit(server int, args RequestVoteArgs, replyCh chan<- RequestVoteReply) {
	var reply RequestVoteReply
	if !rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
		reply.Err, reply.Server = ErrRPCFail, server
	}
	replyCh <- reply
}

// 候选者竞争赢得选举
func (rf *Raft) campaign() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.leaderId = -1     // 确认这里没有leader
	rf.state = Candidate // 转变状态
	rf.currentTerm += 1  // 提升当前Term
	rf.votedFor = rf.me  // 投给自己
	currentTerm, lastLogIndex, me := rf.currentTerm, rf.logIndex-1, rf.me
	lastLogTerm := rf.getEntry(lastLogIndex).LogTerm
	DPrintf("%d at %d start election, last index %d last term %d last entry %v",
		rf.me, rf.currentTerm, lastLogIndex, lastLogTerm, rf.getEntry(lastLogIndex))
	rf.persist()
	args := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	electionDuration := newRandDuration(ElectionTimeout)
	rf.resetElectionTimer(electionDuration)
	timer := time.After(electionDuration) // 没有足够人数，选举超时
	rf.mu.Unlock()
	replyCh := make(chan RequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != me {
			go rf.solicit(i, args, replyCh)
		}
	}
	voteCount, threshold := 0, len(rf.peers)/2 // 计算投票数
	for voteCount < threshold {
		select {
		case <-rf.shutdown:
			return
		case <-timer: // 选举超时
			return
		case reply := <-replyCh:
			if reply.Err != OK {
				go rf.solicit(reply.Server, args, replyCh)
			} else if reply.VoteGranted {
				voteCount += 1
			} else { //检查server是否过时
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.stepDown(reply.Term)
				}
				rf.mu.Unlock()
			}
		}
	}
	// 收到足够选票
	rf.mu.Lock()
	if rf.state == Candidate { // 检查是否对应节点状态为Candidate
		DPrintf("CANDIDATE: %d receive enough vote and becoming a new leader", rf.me)
		rf.state = Leader
		rf.initIndex()
		go rf.tick()
		go rf.notifyNewLeader()
	}
	rf.mu.Unlock()
}


func (rf *Raft) sendLogEntry(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[follower] <= rf.lastIncludedIndex {
		go rf.sendSnapshot(follower)
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[follower] - 1
	prevLogTerm := rf.getEntry(prevLogIndex).LogTerm
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, CommitIndex: rf.commitIndex, Len: 0, Entries: nil}
	if rf.nextIndex[follower] < rf.logIndex {
		entries := rf.getRangeEntry(prevLogIndex+1, rf.logIndex)
		args.Entries, args.Len = entries, len(entries)
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	if rf.peers[follower].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term>rf.currentTerm{//leader节点已经过时
				rf.stepDown(reply.Term)
			} else { // follower节点和leader节点并不一致
				rf.nextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.logIndex))
				if rf.nextIndex[follower] <= rf.lastIncludedIndex {
					go rf.sendSnapshot(follower)
				}
			}
		} else { //reply成功
			prevLogIndex, logEntriesLen := args.PrevLogIndex, args.Len
			if prevLogIndex+logEntriesLen >= rf.nextIndex[follower] {
				rf.nextIndex[follower] = prevLogIndex + logEntriesLen + 1
				rf.matchIndex[follower] = prevLogIndex + logEntriesLen
			}
			toCommitIndex := prevLogIndex + logEntriesLen
			if rf.canCommit(toCommitIndex) {
				rf.commitIndex = toCommitIndex
				rf.persist()
				rf.notifyApplyCh <- struct{}{}
			}
		}
		rf.mu.Unlock()
	}
}

// 分发快照
func (rf *Raft) sendSnapshot(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.getEntry(rf.lastIncludedIndex).LogTerm, Data: rf.persister.ReadSnapshot()}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if rf.peers[follower].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
		} else {
			rf.nextIndex[follower] = Max(rf.nextIndex[follower], rf.lastIncludedIndex+1)
			rf.matchIndex[follower] = Max(rf.matchIndex[follower], rf.lastIncludedIndex)
		}
		rf.mu.Unlock()
	}
}


func (rf *Raft) tick() {
	timer := time.NewTimer(AppendEntriesInterval)
	for {
		select {
		case <-rf.shutdown:
			return
		case <-timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go rf.replicate()
			timer.Reset(AppendEntriesInterval)
		}
	}
}

func (rf *Raft) replicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for follower := 0; follower < len(rf.peers); follower++ {
		if follower != rf.me {
			go rf.sendLogEntry(follower)
		}
	}
}


func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock() //保证节点状态的可见性
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	index := rf.logIndex
	entry := LogEntry{LogIndex: index, LogTerm: rf.currentTerm, Command: command}
	if offsetIndex := rf.getOffsetIndex(rf.logIndex); offsetIndex < len(rf.log) {
		rf.log[offsetIndex] = entry
	} else {
		rf.log = append(rf.log, entry)
	}
	rf.matchIndex[rf.me] = rf.logIndex
	rf.logIndex += 1
	rf.persist()
	go rf.replicate()
	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	close(rf.shutdown)
	DPrintf("Kill raft %d at %d state: %s lastLogIndex: %d lastLogEntry %v commit index: %d, last applied index: %d",
		rf.me, rf.currentTerm, rf.state, rf.logIndex, rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) apply() {
	for {
		select {
		case <-rf.notifyApplyCh:
			rf.mu.Lock()
			var commandValid bool
			var entries []LogEntry
			if rf.lastApplied < rf.lastIncludedIndex {
				commandValid = false
				rf.lastApplied = rf.lastIncludedIndex
				entries = [] LogEntry{{LogIndex: rf.lastIncludedIndex, LogTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"}}
			} else if rf.lastApplied < rf.logIndex && rf.lastApplied < rf.commitIndex {
				commandValid = true
				entries = rf.getRangeEntry(rf.lastApplied+1, rf.commitIndex+1)
				rf.lastApplied = rf.commitIndex
			}
			rf.persist()
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{CommandValid: commandValid, CommandIndex: entry.LogIndex, CommandTerm: entry.LogTerm, Command: entry.Command}
			}
		case <-rf.shutdown:
			return
		}
	}
}

func (rf *Raft) Replay(startIndex int) {
	rf.mu.Lock()
	if startIndex <= rf.lastIncludedIndex {
		rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: rf.log[0].LogIndex, CommandTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"}
		startIndex = rf.lastIncludedIndex + 1
		rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	}
	entries := append([]LogEntry{}, rf.log[rf.getOffsetIndex(startIndex):rf.getOffsetIndex(rf.lastApplied+1)]...)
	rf.mu.Unlock()
	for i := 0; i < len(entries); i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
	}
	rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: -1, CommandTerm: -1, Command: "ReplayDone"}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.logIndex = 1
	rf.state = Follower // 初始化状态为Follower
	rf.shutdown = make(chan struct{})
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.electionTimer = time.NewTimer(newRandDuration(ElectionTimeout))
	rf.readPersistState()
	go rf.apply()
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.campaign() // 开始新一轮选举
			case <-rf.shutdown:
				return
			}
		}
	}()
	return rf
}