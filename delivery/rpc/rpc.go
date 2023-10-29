package rpc

import (
	"os"
	"strconv"
)

const (
	doneTaskDefinition = "RPCServer.DoneTask"
	getTaskDefinition  = "RPCServer.GetTask"
)

type DoneTaskReply struct {
}

type DoneTaskArgs struct {
	TaskID int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskID     int
	NReduce    int
	Filename   string
	Assignment byte
}

func CoordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
