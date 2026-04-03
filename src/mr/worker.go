package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

var coordSockName string

// ihash determines the reduce task number for a given key.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// ---------------------------------------------------------
// Main Worker Lifecycle
// ---------------------------------------------------------

func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	coordSockName = sockname

	for {
		reply := CallGetTask()

		switch reply.Phase {
		case MapPhase:
			executeMapTask(reply, mapf)
		case ReducePhase:
			executeReduceTask(reply, reducef)
		case WaitPhase:
			time.Sleep(1 * time.Second)
		case ExitPhase:
			os.Exit(0)
		}
	}
}

// ---------------------------------------------------------
// Phase Execution Logic
// ---------------------------------------------------------

func executeMapTask(reply TaskResponse, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(reply.FileName)
	if err != nil {
		log.Fatalf("Worker %v failed to read file %v: %v", os.Getpid(), reply.FileName, err)
	}

	kvArray := mapf(reply.FileName, string(content))

	encoders := make([]*json.Encoder, reply.NReduce)
	tempFiles := make([]*os.File, reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-map-tmp-%v-*", reply.TaskId))
		if err != nil {
			log.Fatalf("Cannot create temp file: %v", err)
		}
		tempFiles[i] = tempFile
		encoders[i] = json.NewEncoder(tempFile)
	}

	for _, kv := range kvArray {
		bucket := ihash(kv.Key) % reply.NReduce
		if err := encoders[bucket].Encode(&kv); err != nil {
			log.Fatalf("Failed to encode JSON: %v", err)
		}
	}

	for i := 0; i < reply.NReduce; i++ {
		tempFiles[i].Close()
		finalName := fmt.Sprintf("mr-%v-%v", reply.TaskId, i)
		os.Rename(tempFiles[i].Name(), finalName)
	}

	CallReportTask(reply.Phase, reply.TaskId)
}

func executeReduceTask(reply TaskResponse, reducef func(string, []string) string) {
	intermediate := gatherIntermediateData(reply.NMap, reply.TaskId)

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-out-tmp-%v-*", reply.TaskId))
	if err != nil {
		log.Fatalf("Cannot create temp output file: %v", err)
	}

	i := 0
	for i < len(intermediate) {
		groupEnd := findGroupEnd(intermediate, i)
		currentKey := intermediate[i].Key

		groupSlice := intermediate[i:groupEnd]
		values := extractValues(groupSlice)

		output := reducef(currentKey, values)
		fmt.Fprintf(tempFile, "%v %v\n", currentKey, output)

		i = groupEnd
	}

	tempFile.Close()
	os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%v", reply.TaskId))

	CallReportTask(reply.Phase, reply.TaskId)
}

func gatherIntermediateData(nMap int, reduceTaskId int) []KeyValue {
	var intermediate []KeyValue

	for m := 0; m < nMap; m++ {
		fileName := fmt.Sprintf("mr-%v-%v", m, reduceTaskId)
		fileData := readIntermediateFile(fileName)
		intermediate = append(intermediate, fileData...)
	}

	return intermediate
}

func readIntermediateFile(fileName string) []KeyValue {
	file, err := os.Open(fileName)
	if err != nil {
		return nil
	}
	defer file.Close()

	var data []KeyValue
	decoder := json.NewDecoder(file)

	for {
		var kv KeyValue
		if err := decoder.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("JSON decoding error in file %s: %v", fileName, err)
		}
		data = append(data, kv)
	}

	return data
}

func findGroupEnd(intermediate []KeyValue, startIndex int) int {
	j := startIndex + 1
	for j < len(intermediate) && intermediate[j].Key == intermediate[startIndex].Key {
		j++
	}
	return j
}

func extractValues(group []KeyValue) []string {
	values := make([]string, len(group))
	for k, kv := range group {
		values[k] = kv.Value
	}
	return values
}

// ---------------------------------------------------------
// RPC Networking
// ---------------------------------------------------------

func CallGetTask() TaskResponse {
	args := TaskRequest{}
	reply := TaskResponse{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		os.Exit(0) // Coordinator unreachable, safely assume job is done
	}

	return reply
}

func CallReportTask(phase TaskPhase, taskId int) {
	args := ReportTaskRequest{Phase: phase, TaskId: taskId}
	reply := ReportTaskResponse{}
	call("Coordinator.ReportTask", &args, &reply)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}

	log.Printf("Worker %d: RPC call %s failed with error: %v", os.Getpid(), rpcname, err)
	return false
}
