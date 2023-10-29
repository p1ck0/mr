package rpc

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/p1ck0/mr"
)

type RPCServer struct {
	addr, network string
	coordinator   mr.Coordinatorer
}

func NewRPCServer(
	addr, network string,
	coordinator mr.Coordinatorer,
) *RPCServer {
	return &RPCServer{
		addr:        addr,
		network:     network,
		coordinator: coordinator,
	}
}

func (s *RPCServer) ListenAndServe(ctx context.Context) error {
	if err := rpc.Register(s); err != nil {
		return err
	}
	rpc.HandleHTTP()
	if s.network == "unix" {
		os.Remove(s.addr)
	}
	l, e := net.Listen(s.network, s.addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	if err := http.Serve(l, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("serve error:", err)
	}
	<-ctx.Done()
	l.Close()

	return nil
}

func (s *RPCServer) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	return s.coordinator.DoneTask(args.TaskID)
}

func (s *RPCServer) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := s.coordinator.GetTask()

	reply.Filename = task.File
	reply.Assignment = task.Assignment
	reply.TaskID = task.ID
	reply.NReduce = task.NReduce
	return nil
}
