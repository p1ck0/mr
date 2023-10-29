package mr

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	mapCount    atomic.Int64
	isDone      atomic.Bool
	mut         sync.Mutex
	queueTask   []*Task
	runningTask map[int]Task
	taskTimeout time.Duration
}

type Coordinatorer interface {
	GetTask() *Task
	DoneTask(taskID int) error
	Done() bool
}

func (c *Coordinator) checkTask(taskID int) {
	timer := time.NewTimer(c.taskTimeout)
	<-timer.C
	c.mut.Lock()
	defer c.mut.Unlock()
	if task, ok := c.runningTask[taskID]; ok {
		if task.Assignment == REDUCE {
			c.queueTask = append(c.queueTask, &task)
		} else {
			newQueue := make([]*Task, 0, len(c.queueTask)+1)
			newQueue = append(newQueue, &task)
			c.queueTask = append(newQueue, c.queueTask...)
		}
		delete(c.runningTask, taskID)
	}
}

// DoneTask marks a task as done in the Coordinator.
//
// taskID: the ID of the task to mark as done.
// Returns an error if the task is not found.
func (c *Coordinator) DoneTask(taskID int) error {
	c.mut.Lock()
	defer c.mut.Unlock()
	task, ok := c.runningTask[taskID]
	if !ok {
		return fmt.Errorf("task %v not found", taskID)
	}
	delete(c.runningTask, taskID)
	if task.Assignment == MAP {
		c.mapCount.Add(-1)
	}
	if len(c.queueTask) == 0 && len(c.runningTask) == 0 {
		c.isDone.Store(true)
	}

	return nil
}

// GetTask returns a task from the Coordinator's queue.
//
// It checks if the coordinator is done and returns an ABORT task if it is.
// Otherwise, it locks the coordinator's mutex and checks if the queue is empty.
// If the queue is empty, it returns a SNOOZE task.
// If the task in the queue is a REDUCE task and there are still map tasks running,
// it returns a SNOOZE task.
// Otherwise, it removes the task from the queue, adds it to the running tasks,
// and returns the task.
func (c *Coordinator) GetTask() *Task {
	if c.isDone.Load() {
		return &Task{Assignment: ABORT}
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	if len(c.queueTask) == 0 {
		return &Task{Assignment: SNOOZE}
	}
	task := c.queueTask[0]
	if task.Assignment == REDUCE && c.mapCount.Load() > 0 {
		return &Task{Assignment: SNOOZE}
	}
	defer func() {
		go c.checkTask(task.ID)
	}()
	c.queueTask = c.queueTask[1:]
	c.runningTask[task.ID] = *task
	return task
}

// Done returns the value of the isDone atomic boolean.
//
// No parameters.
// Returns a boolean value.
func (c *Coordinator) Done() bool {
	return c.isDone.Load()
}

// MakeCoordinator creates a new Coordinator instance.
//
// It takes in a slice of file names, an integer representing the number of reduces,
// and optional options for the Coordinator. It returns a pointer to the newly created Coordinator.
func MakeCoordinator(
	files []string,
	nReduce int,
	opts ...Options,
) *Coordinator {
	option := &options{
		timeout: _defaultTaskTimeout,
	}
	for _, opt := range opts {
		opt.apply(option)
	}

	c := Coordinator{
		queueTask: make([]*Task, 0, len(files)+nReduce),
	}
	taskNum := 0
	for _, file := range files {
		taskNum++
		c.queueTask = append(c.queueTask,
			&Task{
				ID:         taskNum,
				File:       file,
				Assignment: MAP,
				NReduce:    nReduce,
			},
		)
	}

	for i := 0; i < nReduce; i++ {
		taskNum++
		c.queueTask = append(c.queueTask,
			&Task{
				ID:         taskNum,
				File:       fmt.Sprintf(mapFile, i+1),
				Assignment: REDUCE,
				NReduce:    i + 1,
			},
		)
	}

	lenFiles := len(files)

	c.mapCount.Store(int64(lenFiles))
	c.runningTask = make(map[int]Task, lenFiles+nReduce)
	c.taskTimeout = option.timeout

	return &c
}
