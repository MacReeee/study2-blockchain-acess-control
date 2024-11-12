package main

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
)

func TestGenerateAddress(t *testing.T) {
	nodes := make(map[string]string)
	nodes["central"] = "localhost:3000"
	nodes["node1"] = "localhost:3001"
	nodes["node2"] = "localhost:3002"
	nodes["node3"] = "localhost:3003"
	nodes["node4"] = "localhost:3004"

	// store to file
	if _, err := os.Stat("testfile/nodes.json"); err == nil {
		os.Remove("testfile/nodes.json")
	}
	f, err := os.Create("testfile/nodes.json")
	HandleError(err)
	defer f.Close()
	nodesJSON, err := json.Marshal(nodes)
	HandleError(err)
	f.Write(nodesJSON)
}

func TestLoadAddress(t *testing.T) {
	f, err := os.Open("testfile/nodes.json")
	HandleError(err)
	defer f.Close()

	decoder := json.NewDecoder(f)
	nodes := make(map[string]string)
	err = decoder.Decode(&nodes)
	HandleError(err)

	// 输入output.txt查看内容
	if _, err := os.Stat("testfile/output.txt"); err == nil {
		os.Remove("testfile/output.txt")
	}
	testFile, err := os.Create("testfile/output.txt")
	HandleError(err)
	defer testFile.Close()
	tWriter := bufio.NewWriter(testFile)
	for name, addr := range nodes {
		tWriter.WriteString(name + ": " + addr + "\n")
	}
	tWriter.Flush()
}
