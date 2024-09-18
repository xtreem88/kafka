package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type errorCode int16

const (
	NO_ERROR            errorCode = 0
	UNKNOWN_VERSION     errorCode = 35
	API_VERSION_REQUEST uint16    = 18
	FETCH_REQUEST       uint16    = 1
)

// Server represents the Kafka server
type Server struct {
	listener net.Listener
	addr     string
}

// NewServer creates a new Server instance
func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
	}
}

// Start begins listening for incoming connections
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

// handleConnection processes an individual client connection
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

		// Start a new goroutine to process the message concurrently
		go s.processMessage(conn, messageBuf)
	}
}

// processMessage handles the logic for processing a single message
func (s *Server) processMessage(conn net.Conn, messageBuf []byte) {
	apiKey := binary.BigEndian.Uint16(messageBuf[0:2])
	apiVersion := binary.BigEndian.Uint16(messageBuf[2:4])
	fmt.Printf("Parsed API Key: %d\n", apiKey)
	fmt.Printf("Parsed API Version: %d\n", apiVersion)

	correlationID := binary.BigEndian.Uint32(messageBuf[4:8])
	fmt.Printf("Parsed correlation ID: %d\n", correlationID)

	response := s.buildResponse(apiVersion, correlationID, apiKey)
	_, err := conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response:", err)
		return
	}
}

// buildResponse constructs the response to be sent back to the client
func (s *Server) buildResponse(apiVersion uint16, correlationID uint32, apiKey uint16) []byte {
	response := []byte{}

	response = binary.BigEndian.AppendUint32(response, correlationID)

	if apiKey == FETCH_REQUEST {
		response = handleFetchRequest(response)
	} else {
		response = handleApiVersionsRequest(apiVersion, response)
	}

	// Set the response length at the start
	responseLength := uint32(len(response) - 4)

	fmt.Printf("Sending response of length %d: %v\n", responseLength, response)

	return response
}

func handleFetchRequest(response []byte) []byte {
	// .TAG_BUFFER
	// a delimiter for the following fields
	response = append(response, 0)
	// Throttle time (4 bytes)
	response = binary.BigEndian.AppendUint32(response, 0)
	// Error code (2 bytes)
	// no error
	response = binary.BigEndian.AppendUint16(response, 0)
	// Session Id (2 bytes)
	response = binary.BigEndian.AppendUint32(response, 0)
	// responses field
	// 0 elements in the response field
	response = append(response, 0)
	// .TAG_BUFFER
	response = append(response, 0)
	// Prepend the size of the response
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(response)))
	return append(size, response...)
}

func handleApiVersionsRequest(apiVersion uint16, response []byte) []byte {
	errCode := make([]byte, 2)
	if apiVersion > 4 {
		fmt.Println("Invalid API Version, sending UNKNOWN_VERSION")
		binary.BigEndian.PutUint16(errCode, uint16(UNKNOWN_VERSION))
	} else {
		binary.BigEndian.PutUint16(errCode, uint16(NO_ERROR))
	}
	response = append(response, errCode...)
	response = append(response, 3)
	// APIVersion API Key (2 bytes)
	response = binary.BigEndian.AppendUint16(response, API_VERSION_REQUEST)
	// Min Version (2 bytes)
	response = binary.BigEndian.AppendUint16(response, 0)
	// Max Version (2 bytes)
	response = binary.BigEndian.AppendUint16(response, 4)
	// .TAG_BUFFER
	// a delimiter for the following field
	response = append(response, 0)
	// Fetch API Key (2 bytes)
	response = binary.BigEndian.AppendUint16(response, FETCH_REQUEST)
	// Min Version (2 bytes)
	response = binary.BigEndian.AppendUint16(response, 0)
	// Max Version (2 bytes)
	response = binary.BigEndian.AppendUint16(response, 16)
	// .TAG_BUFFER
	// a delimiter for the following fields
	response = append(response, 0)
	// Throttle time (4 bytes)
	response = binary.BigEndian.AppendUint32(response, 0)
	// .TAG_BUFFER
	response = append(response, 0)
	// Prepend the size of the response
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(response)))
	return append(size, response...)
}
