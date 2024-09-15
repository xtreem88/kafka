package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/server"
)

func main() {
	server := server.NewServer("0.0.0.0:9092")
	if err := server.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
