package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type errorCode int16

const (
	NO_ERROR        errorCode = 0
	UNKNOWN_VERSION errorCode = 35
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

		apiVersion := binary.BigEndian.Uint16(buffer[4+2 : 4+2+2])
		fmt.Printf("Parsed API Version: %d\n", apiVersion)

		correlationID := binary.BigEndian.Uint32(buffer[4+2+2 : 4+2+2+4]) // length (4) + api_key(2) + api_version(2)... correlationID is 4 byte
		fmt.Printf("Parsed correlation ID: %d\n", correlationID)

		response := []byte{0x00, 0x00, 0x00, 0x13}

		corrId := make([]byte, 4)
		binary.BigEndian.PutUint32(corrId, uint32(correlationID))
		response = append(response, corrId...)

		errCode := make([]byte, 2)
		if apiVersion != 0 && apiVersion != 1 && apiVersion != 2 && apiVersion != 3 && apiVersion != 4 {
			fmt.Println("Invalid API Version, sending UNSUPPORTED_VERSION")
			binary.BigEndian.PutUint16(errCode, uint16(UNKNOWN_VERSION))
		} else {
			binary.BigEndian.PutUint16(errCode, uint16(NO_ERROR))
		}
		response = append(response, errCode...)

		response = append(response, 0x02)       // api versions count
		response = append(response, 0x00, 0x12) // api version = 18
		response = append(response, 0x00, 0x04)
		response = append(response, 0x00, 0x04)
		response = append(response, 0x00)
		response = append(response, 0x00, 0x00, 0x00, 0x00)
		response = append(response, 0x00)

		fmt.Printf("===========%v\n", response)
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

		conn.Close()
	}
}
