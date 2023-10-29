package rpc

import (
	"net/rpc"

	"github.com/p1ck0/mr"
)

type RPCClient struct {
	addr    string
	network string
}

func NewRPCClient(addr, network string) *RPCClient {
	return &RPCClient{addr: addr, network: network}
}

func (s *RPCClient) call(rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP(s.network, s.addr)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}

func (s *RPCClient) GetTask() (*mr.Task, error) {
	args := &GetTaskArgs{}
	reply := &GetTaskReply{}
	if err := s.call(getTaskDefinition, args, reply); err != nil {
		return nil, err
	}

	return &mr.Task{
		File:       reply.Filename,
		Assignment: reply.Assignment,
		ID:         reply.TaskID,
		NReduce:    reply.NReduce,
	}, nil
}

func (s *RPCClient) DoneTask(taskID int) error {
	args := &DoneTaskArgs{TaskID: taskID}
	reply := &DoneTaskReply{}
	if err := s.call(doneTaskDefinition, args, reply); err != nil {
		return err
	}

	return nil
}
