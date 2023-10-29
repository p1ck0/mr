package mr

import (
	"hash/fnv"
	"os"
	"time"

	"log/slog"
)

const _defaultSnoozeTime = 1 * time.Second

type Delivery interface {
	GetTask() (*Task, error)
	DoneTask(taskID int) error
}

type MapReducer interface {
	MapAndShuffle(filename string, nReduce int) [][]KeyValue
	StoreMap(kvs [][]KeyValue)
	ReadMap(src string) ([]KeyValue, error)
	Reduce(kv []KeyValue) []KeyValue
	StoreReduce(nReduce int, src string, kv []KeyValue) error
}

// New creates a new Worker instance.
//
// It takes a MapReducer, a Delivery, and optional Options as parameters.
// It returns a pointer to a Worker.
func New(
	mapReduce MapReducer,
	delivery Delivery,
	opts ...Options,
) *Worker {
	options := &options{
		timeout: _defaultSnoozeTime,
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	handler := slog.NewJSONHandler(os.Stdout, logOpts)

	logger := slog.New(handler)
	return &Worker{
		logger:     logger,
		mapReduce:  mapReduce,
		delivery:   delivery,
		snoozeTime: options.timeout,
	}
}

type Worker struct {
	logger     *slog.Logger
	mapReduce  MapReducer
	delivery   Delivery
	snoozeTime time.Duration
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Run runs the worker.
//
// It retrieves tasks from the delivery and performs the corresponding actions based on the task assignment.
// The function does not take any parameters and does not return any values.
func (m *Worker) Run() {
	defer m.logger.Info("worker exit")
	for {
		task, err := m.delivery.GetTask()
		if err != nil {
			m.logger.Error(err.Error())
			return
		}

		switch task.Assignment {
		case MAP:
			shuffleData := m.mapReduce.MapAndShuffle(task.File, task.NReduce)

			if err := m.delivery.DoneTask(task.ID); err != nil {
				m.logger.Error(err.Error())
				continue
			}

			m.mapReduce.StoreMap(shuffleData)
		case REDUCE:
			intermediate, err := m.mapReduce.ReadMap(task.File)
			if err != nil {
				m.logger.Error(err.Error())
				continue
			}

			output := m.mapReduce.Reduce(intermediate)

			if err := m.delivery.DoneTask(task.ID); err != nil {
				m.logger.Error(err.Error())
				continue
			}

			if err := m.mapReduce.StoreReduce(task.NReduce, task.File, output); err != nil {
				m.logger.Error(err.Error())
			}
		case SNOOZE:
			time.Sleep(m.snoozeTime)
		case ABORT:
			m.logger.Info("abort")
			return
		}

	}
}
