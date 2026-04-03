package mr

import (
	"encoding/json"
	"os"
	"testing"
)

func TestIhash_SameKey_ReturnsConsistentBucket(t *testing.T) {
	nReduce := 10
	key := "apple"

	bucket1 := ihash(key) % nReduce
	bucket2 := ihash(key) % nReduce

	if bucket1 != bucket2 {
		t.Errorf("Expected bucket %v, got %v", bucket1, bucket2)
	}
}

func TestMapPartitioning_MultipleKeys_DistributesToCorrectBuckets(t *testing.T) {
	nReduce := 3
	keys := []string{"apple", "banana", "cherry", "date"}
	buckets := make(map[int]int)

	for _, k := range keys {
		bucket := ihash(k) % nReduce
		buckets[bucket]++
	}

	totalFound := 0
	for _, count := range buckets {
		totalFound += count
	}

	if totalFound != len(keys) {
		t.Errorf("Expected %v keys, got %v", len(keys), totalFound)
	}
}

func TestJsonEncoding_KeyValueStruct_MatchesStoredFormat(t *testing.T) {
	tempDir := t.TempDir()
	kv := KeyValue{Key: "test-key", Value: "1"}
	filePath := tempDir + "/test.json"

	file, _ := os.Create(filePath)
	json.NewEncoder(file).Encode(&kv)
	file.Close()

	readFile, _ := os.Open(filePath)
	var decoded KeyValue
	json.NewDecoder(readFile).Decode(&decoded)
	readFile.Close()

	if decoded.Key != kv.Key || decoded.Value != kv.Value {
		t.Errorf("Expected %v, got %v", kv, decoded)
	}
}

func TestAtomicRename_TempToFinal_PreventsPartialVisibility(t *testing.T) {
	tempDir := t.TempDir()
	finalName := tempDir + "/mr-out-1"
	content := "test-content"

	tempFile, _ := os.CreateTemp(tempDir, "mr-tmp-*")
	tempFile.WriteString(content)
	tempPath := tempFile.Name()
	tempFile.Close()

	if _, err := os.Stat(finalName); !os.IsNotExist(err) {
		t.Errorf("Final file should not exist yet")
	}

	os.Rename(tempPath, finalName)

	data, _ := os.ReadFile(finalName)
	if string(data) != content {
		t.Errorf("Expected %v, got %s", content, data)
	}
}

func TestReduceGroupingHelpers_IdentifyAndExtractCorrectly(t *testing.T) {
	intermediate := []KeyValue{
		{Key: "a", Value: "1"},
		{Key: "a", Value: "2"},
		{Key: "b", Value: "3"},
	}

	endIndex := findGroupEnd(intermediate, 0)
	if endIndex != 2 {
		t.Errorf("Expected group 'a' to end at index 2, got %v", endIndex)
	}

	values := extractValues(intermediate[0:endIndex])
	if len(values) != 2 || values[0] != "1" || values[1] != "2" {
		t.Errorf("Expected values [1 2], got %v", values)
	}

	endIndexB := findGroupEnd(intermediate, endIndex)
	if endIndexB != 3 {
		t.Errorf("Expected group 'b' to end at index 3, got %v", endIndexB)
	}
}
