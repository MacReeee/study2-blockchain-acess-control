package raft_test

import (
	"context"
	"study2/raft"
	pb "study2/types/raft"
	"testing"
	"time"
)

// TestSingleNodeAppendEntries 测试单节点提交数据的功能
func TestSingleNodeAppendEntries(t *testing.T) {
	// 创建一个单节点Raft集群，作为Leader
	node := raft.NewRaftNode(raft.Leader, "")
	node.Address = "127.0.0.1:5000"
	node.Role = raft.Leader

	// 测试数据
	testData := []byte("Hello, Raft!")

	// 测试提交数据
	err := node.SubmitData(testData)
	if err != nil {
		t.Fatalf("Leader should be able to submit data, got error: %v", err)
	}

	// 检查日志是否包含提交的数据
	if len(node.Logs) != 1 {
		t.Fatalf("Expected log to contain one entry, got %d entries", len(node.Logs))
	}
	if string(node.Logs[0].Data) != string(testData) {
		t.Fatalf("Expected log entry data to be '%s', got '%s'", testData, node.Logs[0].Data)
	}

	// 检查CommitIndex是否更新
	if node.CommitIndex != 0 {
		t.Fatalf("Expected CommitIndex to be 0, got %d", node.CommitIndex)
	}

	// 模拟等待一段时间（以确保异步提交完成）
	time.Sleep(2 * time.Second)

	// 验证提交数据后，CommitIndex是否正确
	if node.CommitIndex != 0 {
		t.Fatalf("Expected CommitIndex to be 0 for single node, got %d", node.CommitIndex)
	}
}

// TestFollowerRejectSubmitData 测试Follower拒绝提交数据的情况
func TestFollowerRejectSubmitData(t *testing.T) {
	// 创建一个单节点Raft集群，作为Follower
	node := raft.NewRaftNode(raft.Follower, "")
	node.Address = "127.0.0.1:5001"
	node.Role = raft.Follower

	// 尝试提交数据
	testData := []byte("Hello, Raft!")
	err := node.SubmitData(testData)

	// 检查是否返回错误
	if err == nil {
		t.Fatal("Expected error when Follower tries to submit data, got nil")
	}
}

// TestAppendEntries 测试AppendEntries在单节点的工作情况
func TestAppendEntries(t *testing.T) {
	node := raft.NewRaftNode(raft.Follower, "")
	node.Address = "127.0.0.1:5002"
	node.Role = raft.Follower
	node.Term = 1

	// 创建一个AppendEntryRequest模拟日志追加
	entry := &pb.LogEntry{
		Term:  1,
		Index: 0,
		Data:  []byte("test log entry"),
	}

	req := &pb.AppendEntryRequest{
		Term:          1,
		LeaderAddress: "127.0.0.1:5002",
		Entry:         []*pb.LogEntry{entry},
		PrevLogIndex:  -1,
		PrevLogTerm:   0,
		LeaderCommit:  0,
	}

	// 执行AppendEntries
	resp, err := node.AppendEntry(context.Background(), req)
	if err != nil {
		t.Fatalf("Expected no error from AppendEntries, got %v", err)
	}

	// 检查AppendEntries的响应
	if !resp.Success {
		t.Fatal("Expected AppendEntries to succeed, got failure")
	}
	if node.Term != 1 {
		t.Fatalf("Expected node Term to be 1, got %d", node.Term)
	}
	if node.CommitIndex != 0 {
		t.Fatalf("Expected CommitIndex to be 0, got %d", node.CommitIndex)
	}

	// 验证日志条目是否正确追加
	if len(node.Logs) != 1 {
		t.Fatalf("Expected log to contain one entry, got %d entries", len(node.Logs))
	}
	if string(node.Logs[0].Data) != "test log entry" {
		t.Fatalf("Expected log entry data to be 'test log entry', got '%s'", node.Logs[0].Data)
	}
}
