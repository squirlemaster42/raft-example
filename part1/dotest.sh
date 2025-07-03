#!/bin/bash
set -ex

logfile=~/temp/rlog

go test -v -race -run $@ |& tee ${logfile}

fo run ../tools/raft-testlog-viz/main.go < ${logfil}
