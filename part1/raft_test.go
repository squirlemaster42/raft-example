package raftexample

import "testing"

func TestElectionBasic(t *testing.T) {
    h := NewHarness(t, 3)
    defer h.Shutdown()

    h.CheckSingleLeader()
}
