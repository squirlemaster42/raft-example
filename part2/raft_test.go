package raftexample

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestElectionBasic(t *testing.T) {
    h := NewHarness(t, 3)
    defer h.Shutdown()

    h.CheckSingleLeader()
}

func TestElectionLeaderDisconnect(t *testing.T) {
    h := NewHarness(t, 3)
    defer h.Shutdown()

    origLeaderId, origTerm := h.CheckSingleLeader()

    h.DisconnectPeer(origLeaderId)
    sleepMs(350)

    newLeaderId, newTerm := h.CheckSingleLeader()
    if newLeaderId == origLeaderId {
        t.Errorf("want new leader to be different from orig leader")
    }

    if newTerm <= origTerm {
        t.Errorf("want newTerm <= origTerm, got new term: %d and orig term: %d", newTerm, origTerm)
    }
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
    h := NewHarness(t, 3)
    defer h.Shutdown()

    origLeaderId, _ := h.CheckSingleLeader()

    h.DisconnectPeer(origLeaderId)
    otherId := (origLeaderId + 1) % 3
    h.DisconnectPeer(otherId)

    sleepMs(450)
    h.CheckNoLeader()

    h.ReconnectPeer(otherId)
    h.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
    numNodes := 3
    h := NewHarness(t, numNodes)
    defer h.Shutdown()

    sleepMs(100)
    for i := range 3 {
        h.DisconnectPeer(i)
    }
    sleepMs(450)

    for i := range 3 {
        h.ReconnectPeer(i)
    }
    h.CheckSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
    h := NewHarness(t, 3)
    defer h.Shutdown()
    origLeaderId, _ := h.CheckSingleLeader()

    h.DisconnectPeer(origLeaderId)

    sleepMs(350)
    newLeaderId, newTerm := h.CheckSingleLeader()

    h.ReconnectPeer(origLeaderId)
    sleepMs(150)

    againLeaderId, againTerm := h.CheckSingleLeader()

    if newLeaderId != againLeaderId {
        t.Errorf("again leader id got %d, want %d", againLeaderId, newLeaderId)
    }
    if againTerm != newTerm {
        t.Errorf("again term got %d, want %d", againTerm, newTerm)
    }
}

func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
    defer leaktest.CheckTimeout(t, 100 * time.Millisecond)()

    h := NewHarness(t, 5)
    defer h.Shutdown()
    origLeaderId, _ := h.CheckSingleLeader()

    h.DisconnectPeer(origLeaderId)

    sleepMs(150)
    newLeaderId, newTerm := h.CheckSingleLeader()

    h.ReconnectPeer(origLeaderId)
    sleepMs(150)

    againLeaderId, againTerm := h.CheckSingleLeader()

    if newLeaderId != againLeaderId {
        t.Errorf("again leader id got %d, want %d", againLeaderId, newLeaderId)
    }
    if againTerm != newTerm {
        t.Errorf("again term got %d, want %d", againTerm, newTerm)
    }
}

func TestElectionFollowerComesBack(t *testing.T) {
    defer leaktest.CheckTimeout(t, 100 * time.Millisecond)()

    h := NewHarness(t, 3)
    defer h.Shutdown()

    origLeaderId, origTerm := h.CheckSingleLeader()

    otherId := (origLeaderId + 1) % 3
    h.DisconnectPeer(otherId)
    time.Sleep(650 * time.Millisecond)
    h.ReconnectPeer(otherId)
    sleepMs(150)

    _, newTerm := h.CheckSingleLeader()
    if newTerm <= origTerm {
        t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
    }
}

func TestElectionDisconnect(t *testing.T) {
    defer leaktest.CheckTimeout(t, 100 * time.Millisecond)()

    h := NewHarness(t, 3)
    defer h.Shutdown()

    for range 5 {
        leaderId, _ := h.CheckSingleLeader()

        h.DisconnectPeer(leaderId)
        otherId := (leaderId + 1) % 3
        h.DisconnectPeer(otherId)
        sleepMs(310)
        h.CheckNoLeader()

        h.ReconnectPeer(otherId)
        h.ReconnectPeer(leaderId)

        sleepMs(150)
    }
}
