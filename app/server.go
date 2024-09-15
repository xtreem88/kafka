package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	fmt.Println("Starting Kafka server...")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err)
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			os.Exit(1)
		}

		buffer := make([]byte, 1024)
		_, err = conn.Read(buffer)
		if err != nil && err != io.EOF {
			fmt.Println("Error reading from connection:", err)
			conn.Close()
			continue
		}

		response := make([]byte, 8)

		binary.BigEndian.PutUint32(response[0:4], 4)

		binary.BigEndian.PutUint32(response[4:8], 7)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing response:", err)
		}

		conn.Close()
	}
}
