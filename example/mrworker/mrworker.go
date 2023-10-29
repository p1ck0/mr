package main

import (
	"fmt"
	"log"
	"os"
	"plugin"

	"github.com/p1ck0/mr"
	"github.com/p1ck0/mr/delivery/rpc"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])
	rpcClient := rpc.NewRPCClient(rpc.CoordinatorSock(), "unix")

	worker := mr.NewMapReduce(mapf, reducef)
	manager := mr.New(worker, rpcClient)

	manager.Run()
}

// load the application Map and Reduce functions
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		fmt.Println(err)
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
