#!/bin/bash
set -ex

logfile=~/tmp/rlog

go test -v -race -run $@ |& tee ${logfile}

go run ../tools/raft-testlog-viz/main.go < ${logfil}
