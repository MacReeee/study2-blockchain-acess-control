package main

import "fmt"

func main() {
	bc := NewBlockchain()
	defer bc.db.Close()

	cli := CLI{bc}
	// cli.Run()

	for {
		var command []byte
		var data []byte
		fmt.Print("Enter command: ")
		fmt.Scanln(&command)
		fmt.Print("Enter data: ")
		fmt.Scanln(&data)
		cli.Command(command, data)
	}
}
