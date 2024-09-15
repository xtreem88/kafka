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
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		var lengthBuf [4]byte
		_, err := io.ReadFull(conn, lengthBuf[:])
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading length from connection:", err)
			}
			return
		}
		messageLength := binary.BigEndian.Uint32(lengthBuf[:])
		fmt.Printf("Received message of length: %d\n", messageLength)

		messageBuf := make([]byte, messageLength)
		_, err = io.ReadFull(conn, messageBuf)
		if err != nil {
			fmt.Println("Error reading message from connection:", err)
			return
		}

		if len(messageBuf) < 8 {
			fmt.Println("Message too short")
			continue
		}

		apiKey := binary.BigEndian.Uint16(messageBuf[0:2])
		apiVersion := binary.BigEndian.Uint16(messageBuf[2:4])
		fmt.Printf("Parsed API Key: %d\n", apiKey)
		fmt.Printf("Parsed API Version: %d\n", apiVersion)

		correlationID := binary.BigEndian.Uint32(messageBuf[4:8])
		fmt.Printf("Parsed correlation ID: %d\n", correlationID)

		response := make([]byte, 4)

		corrId := make([]byte, 4)
		binary.BigEndian.PutUint32(corrId, correlationID)
		response = append(response, corrId...)

		errCode := make([]byte, 2)
		if apiVersion != 0 && apiVersion != 1 && apiVersion != 2 && apiVersion != 3 && apiVersion != 4 {
			fmt.Println("Invalid API Version, sending UNKNOWN_VERSION")
			binary.BigEndian.PutUint16(errCode, uint16(UNKNOWN_VERSION))
		} else {
			binary.BigEndian.PutUint16(errCode, uint16(NO_ERROR))
		}
		response = append(response, errCode...)

		response = append(response, 0x02)                   // API versions count
		response = append(response, 0x00, 0x12)             // API key = 18
		response = append(response, 0x00, 0x00)             // Min API version
		response = append(response, 0x00, 0x04)             // Max API version
		response = append(response, 0x00)                   // TAG_BUFFER
		response = append(response, 0x00, 0x00, 0x00, 0x00) // Throttle time (ms)
		response = append(response, 0x00)                   // TAG_BUFFER

		responseLength := uint32(len(response) - 4)
		binary.BigEndian.PutUint32(response[0:4], responseLength)

		fmt.Printf("Sending response of length %d: %v\n", responseLength, response)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing response:", err)
			return
		}
	}
}
