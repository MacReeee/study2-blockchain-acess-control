// Description: raft节点的实现
// 简易Raft实现，仅保留维护数据一致性的核心逻辑，不包含选举、心跳、持久化等功能

package raft

import (
	"context"
	"fmt"
	"log"
	pb "study2/types/raft"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Role int

const (
	Follower Role = iota
	Leader
)

var timeout = 5 * time.Second

type RaftNode struct {
	sync.Mutex                                          // 保证某些情况下的并发安全
	pb.UnimplementedRaftServer                          // 保证实现了 RaftServer 接口
	Id                         int32                    // 节点ID
	Role                       Role                     // 当前节点的角色
	Term                       int32                    // 当前任期
	CommitIndex                int32                    // 已提交的日志索引
	logs                       []pb.LogEntry            // 日志
	peers                      []string                 // 集群中的其他节点地址
	clients                    map[string]pb.RaftClient // 与其他节点的gRPC客户端
}

func NewRaftNode(role Role, peers []string) *RaftNode {
	raft := &RaftNode{
		Role:        role,
		Term:        0,
		CommitIndex: 0,
		logs:        make([]pb.LogEntry, 0),
		peers:       peers,
		clients:     make(map[string]pb.RaftClient),
	}
	return raft
}

// 实现RaftServer接口，执行一致性操作
func (r *RaftNode) AppendEntries(ctx context.Context, req *pb.AppendEntryRequest) (*pb.AppendEntryResponse, error) {
	r.Lock()
	defer r.Unlock()

	// 作为Follower处理AppendEntries请求
	if r.Role == Follower {
		log.Println("Follower接收到AppendEntries请求")

		// 1. 检查请求的任期是否大于当前任期
		if req.Term < r.Term {
			return &pb.AppendEntryResponse{Term: r.Term, Success: false}, fmt.Errorf("请求的任期小于节点 [%d] 的当前任期 [%d], 请求任期: [%d]", r.Id, r.Term, req.Term)
		}
		r.Term = req.Term

		// 2. 检查 PrevLogIndex 和 PrevLogTerm 是否匹配
		if int(req.PrevLogIndex) >= len(r.logs) || (req.PrevLogIndex >= 0 && req.PrevLogTerm != r.logs[req.PrevLogIndex].Term) {
			// 日志不一致，返回失败，有日志缺失
			matchedIndex := int32(len(r.logs) - 1)
			// 向主节点请求缺失的日志
			return &pb.AppendEntryResponse{Term: r.Term, Success: false, MatchIndex: matchedIndex}, fmt.Errorf("日志缺失, 节点 [%d] 的日志长度: %d, 请求的PrevLogIndex: %d", r.Id, len(r.logs), req.PrevLogIndex)
		}

		// 3. 如果匹配，删除从 PrevLogIndex + 1 开始的所有冲突日志
		r.logs = r.logs[:req.PrevLogIndex+1]

		// 4. 追加新的日志条目
		if req.Entry != nil {
			r.logs = append(r.logs, *req.Entry)
		}

		// 5. 更新 CommitIndex 为 LeaderCommit 和新日志条目的最小值
		lastNewIndex := req.PrevLogIndex + 1
		if req.LeaderCommit > r.CommitIndex {
			r.CommitIndex = min(req.LeaderCommit, lastNewIndex)
		}

		return &pb.AppendEntryResponse{Term: r.Term, Success: true, MatchIndex: lastNewIndex}, nil
	}

	log.Println("Leader接收到AppendEntries请求 - ignore")
	return &pb.AppendEntryResponse{Term: r.Term, Success: true}, nil
}

// 提供接口供外部调用，接收客户端的写请求
func (r *RaftNode) SubmitData(data []byte) error {
	r.Lock()
	defer r.Unlock()

	if r.Role != Leader {
		log.Println("非主节点接收到写请求")
		return fmt.Errorf("当前节点不是Leader")
	}

	// 创建新的日志条目
	NewEntry := pb.LogEntry{
		Term:  r.Term,
		Data:  data,
		Index: int32(len(r.logs)),
	}

	// 1. 将日志条目追加到Leader本地日志中
	r.logs = append(r.logs, NewEntry)

	// 2. 向集群中其他节点发送AppendEntries请求
	successCounter := make(chan int, len(r.peers))
	successCounterCtx, successCounterCancelFunc := context.WithTimeout(context.Background(), timeout)
	defer successCounterCancelFunc()
	for _, peer := range r.peers {
		req := &pb.AppendEntryRequest{
			Term:         r.Term,
			LeaderId:     r.Id,
			Entry:        &NewEntry,
			PrevLogIndex: NewEntry.Index - 1,
			PrevLogTerm:  r.logs[NewEntry.Index-1].Term,
			LeaderCommit: r.CommitIndex,
		}

		go func(peer string) {
			client, err := r.getClient(peer)
			if err != nil {
				log.Printf("获取与节点%s的gRPC客户端失败: %v\n", peer, err)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			res, err := client.AppendEntry(ctx, req)
			if err == nil && res.Success {
				r.Lock()
				successCounter <- 1
				r.Unlock()
			} else {
				log.Printf("向节点 [%s] 发送AppendEntries请求失败: %v\n", peer, err)
			}
		}(peer)
	}

	// 3. 如果大多数节点都成功添加了日志条目，则提交日志
	counter := 1
	for {
		select {
		case <-successCounterCtx.Done():
			log.Printf("提交日志超时, index: [%d]", NewEntry.Index)
			return fmt.Errorf("提交日志超时")
		case <-successCounter:
			counter++
			if counter > len(r.peers)/2 {
				r.CommitIndex = NewEntry.Index
				log.Printf("提交日志成功, index: [%d]", NewEntry.Index)
				// 通知其他节点提交日志
				r.notifyCommit()
				return nil
			}
		}
	}
}

// 获取与指定节点的gRPC客户端
func (r *RaftNode) getClient(peer string) (pb.RaftClient, error) {
	if client, ok := r.clients[peer]; ok {
		return client, nil
	}

	// 创建gRPC客户端
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewRaftClient(conn)
	r.clients[peer] = client

	return client, nil
}

// 通知其他节点提交日志
func (r *RaftNode) notifyCommit() {
	req := &pb.AppendEntryRequest{
		Term:         r.Term,
		LeaderId:     r.Id,
		Entry:        nil, // 不附带新日志
		PrevLogIndex: r.CommitIndex - 1,
		PrevLogTerm:  r.logs[r.CommitIndex-1].Term,
		LeaderCommit: r.CommitIndex,
	}
	for _, peer := range r.peers {
		go func(peer string) {
			client, err := r.getClient(peer)
			if err != nil {
				log.Printf("获取与节点%s的gRPC客户端失败: %v\n", peer, err)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			_, err = client.AppendEntry(ctx, req)
			if err != nil {
				log.Printf("向节点%s发送AppendEntries请求失败: %v\n", peer, err)
			}
		}(peer)
	}
}
