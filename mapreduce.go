package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sort"
)

type MapReduce struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// NewMapReduce creates a new MapReduce object.
//
// It takes two functions as parameters: mapf and reducef. mapf is a function that
// takes a key-value pair and returns a slice of KeyValue objects. reducef is a
// function that takes a key and a slice of values and returns a string.
//
// The function returns a pointer to a MapReduce object.
func NewMapReduce(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) *MapReduce {
	return &MapReduce{
		mapf:    mapf,
		reducef: reducef,
	}
}

// MapAndShuffle maps the content of a file to key-value pairs and shuffles the data.
//
// filename: the name of the file to be mapped.
// nReduce: the number of reduce tasks.
// [][]KeyValue: a slice of slices containing the shuffled key-value pairs.
func (w *MapReduce) MapAndShuffle(filename string, nReduce int) [][]KeyValue {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	keyValues := w.mapf(filename, string(content))

	shuffleData := make([][]KeyValue, nReduce)
	for _, item := range keyValues {
		index := ihash(item.Key) % nReduce
		shuffleData[index] = append(shuffleData[index], item)
	}

	return shuffleData
}

// StoreMap stores the given key-value pairs in a temporary file.
//
// The function takes a 2D slice of KeyValue structs as input. Each inner
// slice represents a set of key-value pairs. The function iterates over each
// inner slice and writes the key-value pairs to a temporary file. The file
// name is generated using the index of the inner slice. The function uses
// the json.Encoder to encode each key-value pair and writes it to the file.
// If there is an error opening or encoding the data, a warning message is
// logged.
func (w *MapReduce) StoreMap(kvs [][]KeyValue) {
	for i, data := range kvs {
		tmpFileName := fmt.Sprintf("mr-map-%d", i+1)
		file, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
		if err != nil {
			slog.Warn("open file error: %v", err)
			continue
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range data {
			err := enc.Encode(&kv)
			if err != nil {
				slog.Warn("encode error: %v", err)
			}
		}

	}
}

// ReadMap reads the contents of a file and returns a slice of KeyValue structs.
//
// src: the path to the file to be read.
// []KeyValue: a slice of KeyValue structs representing the contents of the file.
// error: an error if the file cannot be opened or read.
func (w *MapReduce) ReadMap(src string) ([]KeyValue, error) {
	file, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(file)
	intermediate := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	file.Close()

	return intermediate, nil
}

// Reduce is a function that takes a slice of KeyValue pairs and returns a slice of KeyValue pairs.
//
// It sorts the input slice of KeyValue pairs by key and then iterates through the sorted slice,
// grouping together consecutive KeyValue pairs with the same key. For each group, it calls the
// reducef function to produce a single output KeyValue pair. The output slice contains all
// the output KeyValue pairs produced by the reducef function.
//
// Parameters:
// - kv: a slice of KeyValue pairs to be reduced.
//
// Returns:
// - a slice of KeyValue pairs produced by the reducef function.
func (w *MapReduce) Reduce(kv []KeyValue) []KeyValue {
	sort.Sort(ByKey(kv))
	i := 0

	output := make([]KeyValue, 0, len(kv))
	for i < len(kv) {
		j := i + 1
		for j < len(kv) && kv[j].Key == kv[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kv[k].Value)
		}
		output = append(output, KeyValue{kv[i].Key, w.reducef(kv[i].Key, values)})

		i = j
	}

	return output
}

// StoreReduce stores the reduced key-value pairs in the output file.
//
// nReduce: the number of reduce tasks.
// src: the source string.
// kv: the key-value pairs to be stored.
// error: an error if any occurred during the process.
func (w *MapReduce) StoreReduce(
	nReduce int,
	src string,
	kv []KeyValue,
) error {
	ofile, err := os.Create(fmt.Sprintf("mr-out-%v", nReduce))
	if err != nil {
		return err
	}
	defer ofile.Close()

	for _, item := range kv {
		if _, err := ofile.Write([]byte(item.Key + " " + item.Value + "\n")); err != nil {
			return err
		}
	}

	return nil
}
