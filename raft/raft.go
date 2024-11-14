// Description: raft节点的实现
// 简易Raft实现，仅保留维护数据一致性的核心逻辑，不包含选举、心跳、持久化等功能

package raft

import (
	"context"
	"fmt"
	"log"
	"net"
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

type ErrorCode int32

const (
	Success          ErrorCode = iota // 表示成功
	TermMismatch                      // 任期不匹配
	LogInconsistency                  // 日志不一致
	LogDuplicate                      // 日志重复
)

var timeout = 2 * time.Hour

type RaftNode struct {
	sync.Mutex                                          // 保证某些情况下的并发安全
	pb.UnimplementedRaftServer                          // 保证实现了 RaftServer 接口
	Address                    string                   // 节点地址
	Role                       Role                     // 当前节点的角色
	Term                       int32                    // 当前任期
	CommitIndex                int32                    // 已提交的日志索引
	Logs                       []*pb.LogEntry           // 日志
	Peers                      []string                 // 集群中的其他节点地址
	Clients                    map[string]pb.RaftClient // 与其他节点的gRPC客户端
	clientsLock                sync.Mutex               // 保证Clients的并发安全
}

func NewRaftNode(role Role, address string) *RaftNode {
	raft := &RaftNode{
		Address: address,
		Role:    role,
		Logs:    make([]*pb.LogEntry, 0),
		Clients: make(map[string]pb.RaftClient),
	}
	// 开启gRPC服务
	go raft.startGRPCServer()
	return raft
}

// 实现RaftServer接口，执行一致性操作
func (r *RaftNode) AppendEntry(ctx context.Context, req *pb.AppendEntryRequest) (*pb.AppendEntryResponse, error) {
	r.Lock()
	defer r.Unlock()

	// 作为Follower处理AppendEntry请求
	if r.Role == Follower {
		log.Println("Follower接收到AppendEntry请求")

		// 1. 检查请求的任期是否大于当前任期
		if req.Term < r.Term {
			log.Printf("请求的任期 [%d] 小于节点 [%s] 的当前任期 [%d]\n", req.Term, r.Address, r.Term)
			return &pb.AppendEntryResponse{
				Term:       r.Term,
				Success:    false,
				MatchIndex: -1, // 任期不匹配，匹配的日志索引返回什么无所谓
				ErrorCode:  pb.ErrorCode_TERM_MISMATCH,
			}, nil // fmt.Errorf("请求的任期小于节点的当前任期")
		}
		r.Term = req.Term

		// 2. 检查日志是否落后以及日志是否一致
		if int(req.PrevLogIndex) > len(r.Logs)-1 || (req.PrevLogIndex >= 0 && req.PrevLogTerm != r.Logs[req.PrevLogIndex].Term) {
			log.Printf("节点 [%s] 的日志不一致\n", r.Address)
			matchedIndex := min(req.PrevLogIndex-1, int32(len(r.Logs)-1)) // 如果条件1满足，说明单纯的日志缺失，matchedIndex = int32(len(r.Logs)-1) 如果条件2满足，说明日志不一致， matchedIndex = req.PrevLogIndex-1
			// 向主节点请求缺失的日志
			return &pb.AppendEntryResponse{
				Term:       r.Term,
				Success:    false,
				MatchIndex: matchedIndex,
				ErrorCode:  pb.ErrorCode_LOG_INCONSISTENCY,
			}, nil // fmt.Errorf("日志缺失或不一致")
		}

		// 3. 检查日志是否重复
		if int(req.PrevLogIndex) < len(r.Logs)-1 && req.Entry != nil { // 如果满足条件二，说明可能是心跳包或提交信息
			// 日志重复，返回日志重复信息
			log.Printf("节点 [%s] 的日志重复, 日志索引 [%d] \n", r.Address, req.Entry.Index)
			matchedIndex := int32(len(r.Logs) - 1)
			return &pb.AppendEntryResponse{
				Term:       r.Term,
				Success:    false,
				MatchIndex: matchedIndex, // 如果日志重复，返回什么其实无所谓，因为日志重复主节点不会处理该错误
				ErrorCode:  pb.ErrorCode_LOG_DUPLICATE,
			}, nil // fmt.Errorf("日志重复")
		}

		// 4. 如果匹配，删除从 PrevLogIndex + 1 开始的所有冲突日志，保证日志一致，并追加新的日志条目
		if req.Entry != nil {
			r.Logs = r.Logs[:req.PrevLogIndex+1]
			r.Logs = append(r.Logs, req.Entry)
		}

		log.Printf("节点 [%s] 的日志长度: %d\n", r.Address, len(r.Logs))

		// 5. 更新 CommitIndex 为 LeaderCommit 和自身最大值
		r.CommitIndex = max(req.LeaderCommit, r.CommitIndex)

		return &pb.AppendEntryResponse{
			Term:       r.Term,
			Success:    true,
			MatchIndex: int32(len(r.Logs)) - 1,
			ErrorCode:  pb.ErrorCode_SUCCESS,
		}, nil
	}

	log.Println("Leader接收到AppendEntry请求 - ignore")
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
		Index: int32(len(r.Logs)),
	}

	// 1. 将日志条目追加到Leader本地日志中
	r.Logs = append(r.Logs, &NewEntry)

	// 2. 向集群中其他节点发送AppendEntry请求
	successCounter := make(chan int, len(r.Peers)+1)
	successCounter <- 1
	successCounterCtx, successCounterCancelFunc := context.WithTimeout(context.Background(), timeout)
	defer successCounterCancelFunc()
	for _, peer := range r.Peers {
		var PrevLogTerm int32
		if r.CommitIndex == 0 {
			PrevLogTerm = 0
		} else {
			PrevLogTerm = r.Logs[r.CommitIndex-1].Term
		}
		req := &pb.AppendEntryRequest{
			Term:          r.Term,
			LeaderAddress: r.Address,
			Entry:         &NewEntry,
			PrevLogIndex:  NewEntry.Index - 1,
			PrevLogTerm:   PrevLogTerm,
			LeaderCommit:  r.CommitIndex,
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
			if err != nil {
				log.Printf("向节点%s发送 [新日志] AppendEntry请求失败\n", peer)
				return
			}
			// 处理错误
			switch res.ErrorCode {
			case pb.ErrorCode_TERM_MISMATCH:
				// TODO: 说明有节点的任期比Leader大，Leader需要变为Follower，此处先不做处理
				log.Println("存在任期大于自身的节点")
			case pb.ErrorCode_LOG_INCONSISTENCY:
				// 需要向其补发缺失的日志
				log.Printf("向节点%s补发日志索引%d及之后的日志\n", peer, res.MatchIndex+1)
				r.HandleInConsistency(peer, res.MatchIndex)
			case pb.ErrorCode_LOG_DUPLICATE:
				// 日志重复，不做处理
				log.Printf("节点%s的日志重复, 日志索引 [%d] \n", peer, NewEntry.Index)
				return
			case pb.ErrorCode_SUCCESS:
				// 任期匹配，日志一致，日志不重复，继续处理
				successCounter <- 1
			}
		}(peer)
	}

	// 3. 如果大多数节点都成功添加了日志条目，则提交日志
	counter := 0
	for {
		select {
		case <-successCounterCtx.Done():
			log.Printf("提交日志超时, index: [%d]", NewEntry.Index)
			return fmt.Errorf("提交日志超时")
		case <-successCounter:
			counter++
			if counter > len(r.Peers)/2 {
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
	if client, ok := r.Clients[peer]; ok {
		return client, nil
	}

	// 创建gRPC客户端
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	r.clientsLock.Lock()
	defer r.clientsLock.Unlock()
	client := pb.NewRaftClient(conn)
	r.Clients[peer] = client

	return client, nil
}

// 通知其他节点提交日志
func (r *RaftNode) notifyCommit() {
	var PrevLogTerm int32
	if r.CommitIndex == 0 {
		PrevLogTerm = 0
	} else {
		PrevLogTerm = r.Logs[r.CommitIndex-1].Term
	}
	req := &pb.AppendEntryRequest{
		Term:          r.Term,
		LeaderAddress: r.Address,
		Entry:         nil, // 不附带新日志
		PrevLogIndex:  r.CommitIndex - 1,
		PrevLogTerm:   PrevLogTerm,
		LeaderCommit:  r.CommitIndex,
	}
	for _, peer := range r.Peers {
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
				log.Printf("向节点%s发送 [提交信息] AppendEntry请求失败\n", peer)
			}
		}(peer)
	}
}

// 启动gRPC服务端
func (r *RaftNode) startGRPCServer() {
	lis, err := net.Listen("tcp", r.Address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", r.Address, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, r)

	log.Printf("Raft node listening on %s", r.Address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server on %s: %v", r.Address, err)
	}
}

func (r *RaftNode) HandleInConsistency(peer string, matchIndex int32) {
	client, err := r.getClient(peer)
	if err != nil {
		log.Printf("获取与节点%s的gRPC客户端失败: %v\n", peer, err)
		return
	}
	currentIndex := matchIndex + 1
	for currentIndex < int32(len(r.Logs)) {
		// 调用client.AppendEntry补发缺失的日志
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		var PrevLogTerm int32
		if currentIndex == 0 {
			PrevLogTerm = -1
		} else {
			PrevLogTerm = r.Logs[currentIndex-1].Term
		}
		res, err := client.AppendEntry(ctx, &pb.AppendEntryRequest{
			Term:          r.Term,
			LeaderAddress: r.Address,
			Entry:         r.Logs[currentIndex],
			PrevLogIndex:  r.Logs[currentIndex].Index - 1,
			PrevLogTerm:   PrevLogTerm,
			LeaderCommit:  r.CommitIndex,
		})
		if err != nil {
			log.Printf("向节点%s发送 [补发缺失] AppendEntry请求失败, 序号 %d\n", peer, currentIndex)
			return
		}
		// 处理错误
		if res.Success {
			currentIndex++
		} else if res.ErrorCode == pb.ErrorCode_LOG_INCONSISTENCY {
			currentIndex = res.MatchIndex + 1
		} else {
			log.Println("未知错误：", res.ErrorCode)
			return
		}
	}
}
