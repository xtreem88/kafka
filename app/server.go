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
		n, err := io.ReadFull(conn, buffer[:12]) // Read at least 12 bytes to get correlationID
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			conn.Close()
			continue
		}
		fmt.Printf("Read %d bytes from connection\n", n)

		correlationID := binary.BigEndian.Uint32(buffer[4+2+2 : 4+2+2+4]) // length (4) + api_key(2) + api_version(2)... correlationID is 4 byte
		fmt.Printf("Parsed correlation ID: %d\n", correlationID)

		response := make([]byte, 8)

		binary.BigEndian.PutUint32(response[0:4], 4)

		binary.BigEndian.PutUint32(response[4:8], correlationID)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing response:", err)
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			err = tcpConn.CloseWrite()
			if err != nil {
				fmt.Println("Error closing write side of the connection:", err)
			}
		}

		// We keep the read side of the connection open to avoid premature reset.
		// The connection will eventually be closed when the client is done.
	}
}
