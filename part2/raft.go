package raftexample

import (
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DebugCM = 1

type LogEntry struct {
    Command any
    Term int
}

type CommitEntry struct {
    Command any
    Index int
    Term int
}

type CMState int

const (
    Follower CMState = iota
    Candidate
    Leader
    Dead
)

func (s CMState) String() string {
    switch s {
    case Follower:
        return "Follower"
    case Candidate:
        return "Candidate"
    case Leader:
        return "Leader"
    case Dead:
        return "Dead"
    default:
        panic("unknown CM state")
    }
}

type ConsensusModule struct {
    mu sync.Mutex
    id int
    peerIds []int
    server *Server
    commitChan chan<- CommitEntry
    newCommitReadyChan chan struct{}
    currentTerm int
    votedFor int
    log []LogEntry
    commitIndex int
    lastApplied int
    state CMState
    electionResetEvent time.Time
    nextIndex map[int]int
    matchIndex map[int]int
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan any, commitChan chan<- CommiCommitEntry) *ConsensusModule {
    cm := new(ConsensusModule)
    cm.id = id
    cm.peerIds = peerIds
    cm.server = server
    cm.commitChan = commitChan
    cm.newCommitReadyChan = make(chan struct{}, 16)
    cm.state = Follower
    cm.votedFor = -1
    cm.commitIndex = -1
    cm.lastApplied = -1
    cm.nextIndex = make(map[int]int)
    cm.matchIndex = make(map[int]int)
    cm.dlog("Created Consensus Module with id %d", id)

    go func() {
        <-ready
        cm.mu.Lock()
        cm.electionResetEvent = time.Now()
        cm.mu.Unlock()
        cm.runElectionTimer()
    }()

    go cm.commitChanSender()
    return cm
}

func (cm *ConsensusModule) Stop() {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    cm.state = Dead
    cm.dlog("becomes Dead")
    cm.dlog("Stopped %d", cm.id)
}

func (cm *ConsensusModule) dlog(format string, args ...any) {
    if DebugCM > 0 {
        format = fmt.Sprintf("[%d] ", cm.id) + format
        log.Printf(format, args...)
    }
}

type RequestVoteArgs struct {
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

type RequestVoteReply struct {
    Term int
    VoteGranted bool
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int

    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
    Success bool
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
    //Create more colisions and reelections if we are testing
    if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
        return time.Duration(150) * time.Millisecond
    } else {
        return time.Duration(150 + rand.Intn(150)) * time.Millisecond
    }
}

func (cm *ConsensusModule) runElectionTimer() {
    timeoutDuration := cm.electionTimeout()
    cm.mu.Lock()
    termStarted := cm.currentTerm
    cm.mu.Unlock()
    cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

    ticker := time.NewTicker(10 * time.Millisecond)
    defer ticker.Stop()
    for {
        <-ticker.C

        cm.mu.Lock()
        if cm.state != Candidate && cm.state != Follower {
            cm.dlog("in election timer state=%s, bailing out", cm.state)
            cm.mu.Unlock()
            return
        }

        if termStarted != cm.currentTerm {
            cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
            cm.mu.Unlock()
            return
        }

        if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
            cm.startElection()
            cm.mu.Unlock()
            return
        }
        cm.mu.Unlock()
    }
}

func (cm *ConsensusModule) startElection() {
    cm.state = Candidate
    cm.currentTerm += 1
    savedCurrentTerm := cm.currentTerm
    cm.electionResetEvent = time.Now()
    cm.votedFor = cm.id
    cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

    votesReceived := 1

    for _, peerId := range cm.peerIds {
        go func(peerId int) {
            args := RequestVoteArgs {
                Term: savedCurrentTerm,
                CandidateId: cm.id,
            }
            var reply RequestVoteReply

            cm.dlog("sending RequestVote to %d: %+v", peerId, args)
            err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply)
            if err == nil {
                cm.mu.Lock()
                defer cm.mu.Unlock()
                cm.dlog("reveived RequestVoteReply %+v", reply)

                if cm.state != Candidate {
                    cm.dlog("while waiting for reply, state=%v", cm.state)
                    return
                }

                if reply.Term > savedCurrentTerm {
                    cm.dlog("term out of date in RequestVoteReply")
                    cm.becomeFollower(reply.Term)
                    return
                } else if reply.Term == savedCurrentTerm {
                    if reply.VoteGranted {
                        votesReceived++
                        if votesReceived * 2 > len(cm.peerIds) + 1 {
                            cm.dlog("wins election with %d votes", votesReceived)
                            cm.startLeader()
                            return
                        }
                    }
                }
            } else {
                cm.dlog("Failed to Request Vote from %d. Error: %+v", peerId, err)
            }
        }(peerId)
    }

    go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
    cm.state = Leader
    cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)

    go func() {
        ticker := time.NewTicker(50 * time.Millisecond)
        defer ticker.Stop()

        //Send heartbeats as long as we are still the leader
        for {
            cm.leaderSendHeartbeats()
            <-ticker.C

            cm.mu.Lock()
            if cm.state != Leader {
                cm.mu.Unlock()
                return
            }
            cm.mu.Unlock()
        }
    }()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
    cm.mu.Lock()
    savedCurrentTerm := cm.currentTerm
    cm.mu.Unlock()

    for _, peerId := range cm.peerIds {
        go func() {
            cm.mu.Lock()
            ni := cm.nextIndex[peerId]
            prevLogIndex := ni - 1
            prevLogTerm := -1
            if prevLogIndex >= 0 {
                prevLogTerm = cm.log[prevLogIndex].Term
            }
            entries := cm.log[ni:]

            args := AppendEntriesArgs {
                Term: savedCurrentTerm,
                LeaderId: cm.id,
                PrevLogIndex: prevLogIndex,
                PrevLogTerm: prevLogTerm,
                Entries: entries,
                LeaderCommit: cm.commitIndex,
            }
            cm.mu.Unlock()
            cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
            var reply AppendEntriesReply
            err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply)
            if err != nil {
                cm.mu.Lock()
                defer cm.mu.Unlock()
                if reply.Term > savedCurrentTerm {
                    cm.dlog("term out of date in heartbeats reply")
                    cm.becomeFollower(reply.Term)
                    return
                }

                if cm.state == Leader && savedCurrentTerm == reply.Term {
                    if reply.Success {
                        cm.nextIndex[peerId] = ni + len(entries)
                        cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
                        cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)

                        savedCommitIndex := cm.commitIndex
                        for i := cm.commitIndex + 1; i < len(cm.log); i++ {
                            if cm.log[i].Term == cm.currentTerm {
                                matchCount := 1
                                for _, peerId := range cm.peerIds {
                                    if cm.matchIndex[peerId] >= i {
                                        matchCount++
                                    }
                                }

                                if matchCount * 2 > len(cm.peerIds) + 1{
                                    cm.commitIndex = i
                                }
                            }
                        }

                        if cm.commitIndex != savedCommitIndex {
                            cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
                            cm.newCommitReadyChan <- struct{}{}
                        }
                    } else {
                        cm.nextIndex[peerId] = ni - 1
                        cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni - 1)
                    }
                }
            } else {
                slog.Error("Failed to call AppendEntries", "Caller", cm.id, "Peer", peerId, "Error", err)
            }
        }()
    }
}

func (cm *ConsensusModule) becomeFollower(term int) {
    cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
    cm.state = Follower
    cm.currentTerm = term
    cm.votedFor = -1
    cm.electionResetEvent = time.Now()

    go cm.runElectionTimer()
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    if cm.state == Dead {
        return nil
    }
    cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

    if args.Term > cm.currentTerm {
        cm.dlog("... term out of date in RequestVote")
        cm.becomeFollower(args.Term)
    }

    if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
        reply.VoteGranted = true
        cm.votedFor = args.CandidateId
        cm.electionResetEvent = time.Now()
    } else {
        reply.VoteGranted = false
    }
    reply.Term = cm.currentTerm
    cm.dlog("... RequestVote reply: %+v from %d", reply, cm.id)
    return nil
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    if cm.state == Dead {
        return nil
    }
    cm.dlog("AppendEntries: %+v", args)

    if args.Term > cm.currentTerm {
        cm.dlog("... term out of date in AppendEntries")
        cm.becomeFollower(args.Term)
    }

    reply.Success = false
    if args.Term == cm.currentTerm {
        if cm.state != Follower {
            cm.becomeFollower(args.Term)
        }
        cm.electionResetEvent = time.Now()
    }

    if args.PrevLogIndex == -1 ||
        (args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
        reply.Success = true

        logInsertIndex := args.PrevLogIndex + 1
        newEntriesIndex := 0

        for {
            if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
                break
            }

            if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
                break
            }

            logInsertIndex++
            newEntriesIndex++
        }

        if newEntriesIndex < len(args.Entries) {
            cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
            cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
            cm.dlog("... log is now: %v", cm.log)
        }

        if args.LeaderCommit > cm.commitIndex {
            cm.commitIndex = min(args.LeaderCommit, len(cm.log) - 1)
            cm.dlog("... setting commitIndex=%d", cm.commitIndex)
            cm.newCommitReadyChan <- struct{}{}
        }
    }

    reply.Term = cm.currentTerm
    cm.dlog("AppendEntries reply: %+v", *reply)
    return nil
}

func (cm *ConsensusModule) commitChanSender() {
    for range cm.newCommitReadyChan {
        cm.mu.Lock()
        savedTerm := cm.currentTerm
        savedLastApplied := cm.lastApplied
        var entries []LogEntry
        if cm.commitIndex > cm.lastApplied {
            entries = cm.log[cm.lastApplied + 1 : cm.commitIndex + 1]
            cm.lastApplied = cm.commitIndex
        }
        cm.mu.Unlock()
        cm.dlog("commitChanSender entries=%c, savedLastApplied=%d", entries, savedLastApplied)

        for i, entry:= range entries {
            cm.commitChan <- CommitEntry{
                Command: entry.Command,
                Index: savedLastApplied + i + 1,
                Term: savedTerm,
            }
        }
    }
    cm.dlog("commitChanSender done")
}

func (cm *ConsensusModule) Submit(command any) bool {
    cm.mu.Lock()
    defer cm.mu.Unlock()

    cm.dlog("Submit received by node in state %v: %v", cm.state, command)
    if cm.state == Leader {
        cm.log = append(cm.log, LogEntry{
            Command: command,
            Term: cm.currentTerm,
        })
        cm.dlog("... log=%v", cm.log)
        return true
    }
    return false
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    return cm.id, cm.currentTerm, cm.state == Leader
}
