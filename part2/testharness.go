package raftexample

import (
	"log"
	"testing"
	"time"
)

func init() {
    log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
    cluster []*Server
    connected []bool
    n int
    t *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
    ns := make([]*Server, n)
    connected := make([]bool, n)
    ready := make(chan any)

    for i := range n {
        peerIds := make([]int, 0)
        for p := range n {
            if p != i {
                peerIds = append(peerIds, p)
            }
        }

        ns[i] = NewServer(i, peerIds, ready)
        ns[i].Serve()
    }

    for i := range n {
        for j := range n {
            if i != j {
                ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
            }
        }
        connected[i] = true
    }
    close(ready)

    return &Harness{
        cluster: ns,
        connected: connected,
        n: n,
        t: t,
    }
}

func (h *Harness) Shutdown() {
    tlog("Shutting down")
    for i := range h.n {
        tlog("Disconnecting %d", i)
        h.cluster[i].DisconnectAll()
        h.connected[i] = false
        tlog("Disconnected %d", i)
    }
    for i := range h.n {
        tlog("Shutting down %d", i)
        h.cluster[i].Shutdown()
        tlog("Shutdown %d", i)
    }
}

func (h *Harness) DisconnectPeer(id int) {
    tlog("Disconnect %d", id)
    h.cluster[id].DisconnectAll()
    for j := range h.n {
        if j != id {
            h.cluster[j].DisconnectPeer(id)
        }
    }
    h.connected[id] = false
}

func (h *Harness) ReconnectPeer(id int) {
    tlog("Reconnect %d", id)
    for j := range h.n {
        if j != id {
            if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
                h.t.Fatal(err)
            }
            if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
                h.t.Fatal(err)
            }
        }
    }
    h.connected[id] = true
}

func (h *Harness) CheckSingleLeader() (int, int) {
    tlog("Checking Single Leader")
    for r := range 5 {
        tlog("Checking for single leader %d", r)
        leaderId := -1
        leaderTerm := -1
        for i := range h.n {
            if h.connected[i] {
                id, term, isLeader := h.cluster[i].cm.Report()
                tlog("Node %d is in term %d and has leader state %+v", id, term, isLeader)
                if isLeader {
                    if leaderId < 0 {
                        leaderId = id
                        leaderTerm = term
                    } else {
                        h.t.Fatalf("both %d and %d think they are leaders", leaderId, id)
                    }
                }
            }
        }
        if leaderId >= 0 {
            tlog("Found single leader %d on term %d", leaderId, leaderTerm)
            return leaderId, leaderTerm
        }
        time.Sleep(150 * time.Millisecond)
    }

    h.t.Fatalf("leader not found")
    return -1, -1
}

func (h *Harness) CheckNoLeader() {
    for i := range h.n {
        if h.connected[i] {
            _, _, isLeader := h.cluster[i].cm.Report()
            if isLeader {
                h.t.Fatalf("server %d is leader; want none", i)
            }
        }
    }
}

func tlog(format string, args ...any) {
    format = "[TEST] " + format
    log.Printf(format, args...)
}

func sleepMs(n int) {
    time.Sleep(time.Duration(n) * time.Millisecond)
}
