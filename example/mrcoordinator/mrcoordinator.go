package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/p1ck0/mr"
	"github.com/p1ck0/mr/delivery/rpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)

	server := rpc.NewRPCServer(rpc.CoordinatorSock(), "unix", m)
	go server.ListenAndServe(ctx)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
	cancel()
}
