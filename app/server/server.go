package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type errorCode int16

const (
	NO_ERROR        errorCode = 0
	UNKNOWN_VERSION errorCode = 35
)

type Server struct {
	listener net.Listener
	addr     string
}

func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
	}
}

func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("Failed to bind to port %s: %v", s.addr, err)
	}
	defer s.listener.Close()
	fmt.Printf("Starting Kafka server on %s...\n", s.addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
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

		s.processMessage(conn, messageBuf)
	}
}

func (s *Server) processMessage(conn net.Conn, messageBuf []byte) {
	apiKey := binary.BigEndian.Uint16(messageBuf[0:2])
	apiVersion := binary.BigEndian.Uint16(messageBuf[2:4])
	fmt.Printf("Parsed API Key: %d\n", apiKey)
	fmt.Printf("Parsed API Version: %d\n", apiVersion)

	correlationID := binary.BigEndian.Uint32(messageBuf[4:8])
	fmt.Printf("Parsed correlation ID: %d\n", correlationID)

	response := s.buildResponse(apiVersion, correlationID)
	_, err := conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response:", err)
		return
	}
}

func (s *Server) buildResponse(apiVersion uint16, correlationID uint32) []byte {
	response := make([]byte, 4)

	corrId := make([]byte, 4)
	binary.BigEndian.PutUint32(corrId, correlationID)
	response = append(response, corrId...)

	errCode := make([]byte, 2)
	if apiVersion > 4 {
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

	return response
}
