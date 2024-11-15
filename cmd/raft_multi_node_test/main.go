package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"study2/raft"
	"time"
)

type NodeConfig struct {
	Role    string   `json:"role"`
	Address string   `json:"address"`
	Peers   []string `json:"peers"`
}

// LoadConfig 从配置文件加载节点配置
func LoadConfig(address string) (*NodeConfig, error) {
	file, err := os.Open("config.json")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var configs []NodeConfig
	if err := json.NewDecoder(file).Decode(&configs); err != nil {
		return nil, err
	}

	for _, config := range configs {
		if config.Address == address {
			return &config, nil
		}
	}
	return nil, fmt.Errorf("no configuration found for address: %s", address)
}

func main() {
	// 设置日志格式，包含文件名和行号
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 定义命令行参数
	addressFlag := flag.String("address", "", "Address of the node, e.g., 127.0.0.1:5000")
	flag.Parse()

	// 检查地址参数是否提供
	if *addressFlag == "" {
		log.Fatal("Node address must be provided")
	}

	// 加载节点配置
	config, err := LoadConfig(*addressFlag)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// 解析节点角色
	var role raft.Role
	if config.Role == "leader" {
		role = raft.Leader
	} else {
		role = raft.Follower
	}

	// 创建 Raft 节点
	node := raft.NewRaftNode(role, config.Address)
	node.Peers = config.Peers

	// 如果是 Leader 节点，尝试提交数据
	MaxTry := 1000000
	if role == raft.Leader {
		time.Sleep(1 * time.Second) // 等待其他节点启动
		for i := 0; i < MaxTry; i++ {
			testData := append([]byte("Log_"), byte(i+65))

			if err := node.SubmitData(testData); err != nil {
				fmt.Printf("Leader failed to submit data: %v\n", err)
			}
			// time.Sleep(time.Second)
			if i%50000 == 0 {
				log.Printf("已发送日志数: [%d] 条数据, 已提交日志数：[%d]", i, node.CommitIndex)
				time.Sleep(100 * time.Millisecond)
			}
		}
	} else {
		go func() {
			for {
				log.Printf("当前日志长度: %d", len(node.Logs))
				if len(node.Logs) >= MaxTry {
					os.Exit(0)
				}
				time.Sleep(5 * time.Second)
			}
		}()
		// 如果是 Follower 节点，保持服务运行等待 Leader 的 AppendEntries 请求
		select {} // 阻塞以保持程序运行
	}
	node.Wg.Wait()
	log.Printf("已提交日志数：[%d]", node.CommitIndex)
	time.Sleep(5 * time.Second)
	fmt.Println("结束")
}
