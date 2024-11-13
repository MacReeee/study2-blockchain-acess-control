package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"study2/raft"
	"time"
)

func main() {
	// 设置日志格式，包含文件名和行号
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 定义命令行参数
	roleFlag := flag.String("role", "follower", "Role of the node: leader or follower")
	addressFlag := flag.String("address", "", "Address of the node, e.g., 127.0.0.1:5000")
	peersFlag := flag.String("peers", "", "Comma-separated list of other node addresses, e.g., 127.0.0.1:5001,127.0.0.1:5002")
	flag.Parse()

	// 检查地址参数是否提供
	if *addressFlag == "" {
		log.Fatal("Node address must be provided")
	}

	// 解析节点角色
	var role raft.Role
	if *roleFlag == "leader" {
		role = raft.Leader
	} else {
		role = raft.Follower
	}

	// 解析其他节点的地址
	peers := strings.Split(*peersFlag, ",")
	if len(peers) == 1 && peers[0] == "" {
		peers = []string{} // 没有其他节点时设置为空切片
	}

	// 创建 Raft 节点
	node := raft.NewRaftNode(role, *addressFlag)
	node.Peers = peers
	time.Sleep(5 * time.Second)

	// 如果是 Leader 节点，尝试提交数据
	if role == raft.Leader {
		time.Sleep(1 * time.Second) // 等待其他节点启动
		for i := 0; i < 3; i++ {
			testData := append([]byte("Hello, Raft!"), byte(i+65))
			fmt.Println("Leader 提交数据...")

			if err := node.SubmitData(testData); err != nil {
				fmt.Printf("Leader failed to submit data: %v\n", err)
			} else {
				fmt.Println("Data submitted by Leader.")
			}
		}

		// 等待日志复制完成
		// time.Sleep(3 * time.Second)
	} else {
		// 如果是 Follower 节点，保持服务运行等待 Leader 的 AppendEntries 请求
		select {} // 阻塞以保持程序运行
	}
}
