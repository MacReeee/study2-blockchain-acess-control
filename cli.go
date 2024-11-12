package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/boltdb/bolt"
)

// CLI responsible for processing command line arguments
type CLI struct {
	bc *Blockchain
}

func (cli *CLI) printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  addblock -data BLOCK_DATA - add a block to the blockchain")
	fmt.Println("  printchain - print all the blocks of the blockchain")
}

func (cli *CLI) validateArgs() {
	if len(os.Args) < 2 {
		cli.printUsage()
		os.Exit(1)
	}
}

func (cli *CLI) addBlock(data string) {
	cli.bc.AddBlock(data)
	fmt.Println("Success!")
}

func (cli *CLI) printChain() {
	bci := cli.bc.Iterator()

	for {
		block := bci.Next()

		fmt.Printf("Prev. hash: %x\n", block.PrevBlockHash)
		fmt.Printf("Data: %s\n", block.Data)
		fmt.Printf("Hash: %x\n", block.Hash)
		pow := NewProofOfWork(block)
		fmt.Printf("PoW: %s\n", strconv.FormatBool(pow.Validate()))
		fmt.Println()

		if len(block.PrevBlockHash) == 0 {
			break
		}
	}
}

// debug: 仅供调试使用，检查数据库文件是否存在以及查看数据库内容
func (cli *CLI) CheckDbFile() {
	bc := cli.bc
	// 检查数据库文件是否存在
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		fmt.Println("Database file does not exist.")
	}
	// 查看数据库内容
	db := bc.db
	// 开启只读事务
	err := db.View(func(tx *bolt.Tx) error {
		// 遍历数据库中的所有bucket
		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			fmt.Printf("Bucket: %s\n", name)
			return nil
		})
		return err
	})
	if err != nil {
		log.Panic(err)
	}
}

// 尝试修改cli模式为直接函数调用
// 解析命令
func (cli *CLI) Command(command []byte, data []byte) {
	// if string(command) == "addblock" {
	// 	cli.addBlock(string(data))
	// } else if string(command) == "printchain" {
	// 	cli.printChain()
	// } else if string(command) == "exit" {
	// 	os.Exit(0)
	// }
	switch string(command) {
	case "addblock":
		cli.addBlock(string(data))
	case "printchain":
		cli.printChain()
	case "exit":
		os.Exit(0)
	case "checkdb":
		cli.CheckDbFile()
	}
}

// Run parses command line arguments and processes commands
func (cli *CLI) Run() {
	cli.validateArgs()

	addBlockCmd := flag.NewFlagSet("addblock", flag.ExitOnError)
	printChainCmd := flag.NewFlagSet("printchain", flag.ExitOnError)

	addBlockData := addBlockCmd.String("data", "", "Block data")

	switch os.Args[1] {
	case "addblock":
		err := addBlockCmd.Parse(os.Args[2:])
		if err != nil {
			log.Panic(err)
		}
	case "printchain":
		err := printChainCmd.Parse(os.Args[2:])
		if err != nil {
			log.Panic(err)
		}
	default:
		cli.printUsage()
		os.Exit(1)
	}

	if addBlockCmd.Parsed() {
		if *addBlockData == "" {
			addBlockCmd.Usage()
			os.Exit(1)
		}
		cli.addBlock(*addBlockData)
	}

	if printChainCmd.Parsed() {
		cli.printChain()
	}
}
