package mr

import "time"

type Task struct {
	ID         int
	Assignment byte
	File       string
	NReduce    int
}

const (
	SNOOZE = iota
	ABORT
	REDUCE
	MAP

	mapFile             = "mr-map-%d"
	_defaultTaskTimeout = 10 * time.Second
)
