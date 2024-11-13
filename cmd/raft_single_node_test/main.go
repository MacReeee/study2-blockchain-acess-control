package main

import (
	"fmt"
	"log"
	"study2/raft"
	"time"
)

func main() {
	// 创建一个单节点Raft集群，作为Leader
	node := raft.NewRaftNode(raft.Leader, "")
	node.Address = "127.0.0.1:5000"
	node.Role = raft.Leader

	// 测试数据
	testData := []byte("Hello, Raft!")

	// 测试提交数据
	fmt.Println("Testing SubmitData...")
	err := node.SubmitData(testData)
	if err != nil {
		log.Fatalf("Leader should be able to submit data, got error: %v", err)
	} else {
		fmt.Println("SubmitData succeeded.")
	}

	// 检查日志是否包含提交的数据
	if len(node.Logs) != 1 {
		log.Fatalf("Expected log to contain one entry, got %d entries", len(node.Logs))
	} else {
		fmt.Println("Log entry count is correct.")
	}

	if string(node.Logs[0].Data) != string(testData) {
		log.Fatalf("Expected log entry data to be '%s', got '%s'", testData, node.Logs[0].Data)
	} else {
		fmt.Println("Log entry data is correct.")
	}

	// 检查CommitIndex是否更新
	if node.CommitIndex != 0 {
		log.Fatalf("Expected CommitIndex to be 0, got %d", node.CommitIndex)
	} else {
		fmt.Println("CommitIndex is correct after submission.")
	}

	// 模拟等待一段时间（以确保异步提交完成）
	fmt.Println("Waiting for asynchronous commit...")
	time.Sleep(2 * time.Second)

	// 验证提交数据后，CommitIndex是否正确
	if node.CommitIndex != 0 {
		log.Fatalf("Expected CommitIndex to be 0 for single node, got %d", node.CommitIndex)
	} else {
		fmt.Println("CommitIndex is correct after asynchronous commit.")
	}

	fmt.Println("Single-node Raft test completed successfully.")

	node.SubmitData([]byte("abc"))
	node.SubmitData([]byte("def"))
	node.SubmitData([]byte("ghi"))
	node.SubmitData([]byte("jkl"))

	for _, entry := range node.Logs {
		fmt.Println(string(entry.Data))
	}
}
