package raftexample

import (
	"log"
	"testing"
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

    for i := 0; i < n; i++ {
        peerIds := make([]int, 0)
        for p := 0; p < n; p++ {
            if p != i {
                peerIds = append(peerIds, p)
            }
        }

        ns[i] = NewServer(i, peerIds, ready)
        ns[i].Serve()
    }

    for i := 0; i < n; i++ {
        for j := 0; j < n; j++ {
            if i != j {
                ns[i].ConnectToPeer(j, ns[i].GetListenAddr())
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
    for i := 0; i < h.n; i++ {
        h.cluster[i].DisconnectAll()
        h.connected[i] = false
    }
    for i := 0; i < h.n; i++ {
        h.cluster[i].Shutdown()
    }
}

func (h *Harness) DisconnectPeer(id int) {
    tlog("Disconnect %d", id)
    h.cluster[id].DisconnectAll()
    for j := 0; j < h.n; j++ {
        if j != id {
            h.cluster[j].DisconnectPeer(id)
        }
    }
    h.connected[id] = false
}

func (h *Harness) ReconnectPeer(id int) {
    tlog("Reconnect %d", id)
    for j := 0; j < h.n; j++ {
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
