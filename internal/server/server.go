package server

import (
	"bytes"
	"io"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/gsralex/dashmq/internal/api"
	"github.com/gsralex/dashmq/internal/config"
	"github.com/gsralex/dashmq/internal/protocol"
	"github.com/gsralex/dashmq/internal/storage"
)

// Server represents the Kafka-compatible server
type Server struct {
	config   *config.Config
	broker   *storage.Broker
	listener net.Listener
	wg       sync.WaitGroup
	stopCh   chan struct{}
}

// New creates a new server instance
func New(cfg *config.Config) *Server {
	return &Server{
		config: cfg,
		broker: storage.NewBroker(1),
		stopCh: make(chan struct{}),
	}
}

// Start starts the server
func (s *Server) Start() error {
	addr := net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.Port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = listener

	log.Printf("Server listening on %s", addr)

	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-s.stopCh:
					return nil
				default:
					log.Printf("Error accepting connection: %v", err)
					continue
				}
			}

			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}
}

// Stop stops the server
func (s *Server) Stop() {
	close(s.stopCh)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	log.Println("Server stopped")
}

// handleConnection handles a client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	log.Printf("New connection from %s", conn.RemoteAddr())

	for {
		select {
		case <-s.stopCh:
			return
		default:
			// Read request size (4 bytes)
			var sizeBuf [4]byte
			if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
				if err != io.EOF {
					log.Printf("Error reading request size: %v", err)
				}
				return
			}

			requestSize := int32(sizeBuf[0])<<24 | int32(sizeBuf[1])<<16 | int32(sizeBuf[2])<<8 | int32(sizeBuf[3])
			if requestSize < 0 || requestSize > 100*1024*1024 { // 100MB limit
				log.Printf("Invalid request size: %d", requestSize)
				return
			}

			// Read request body
			requestBody := make([]byte, requestSize)
			if _, err := io.ReadFull(conn, requestBody); err != nil {
				log.Printf("Error reading request body: %v", err)
				return
			}

			// Process request
			if err := s.processRequest(conn, requestBody); err != nil {
				log.Printf("Error processing request: %v", err)
				return
			}
		}
	}
}

// processRequest processes a single request
func (s *Server) processRequest(conn net.Conn, requestBody []byte) error {
	// Create decoder from request body
	decoder := protocol.NewDecoder(bytes.NewReader(requestBody))

	// Read request header
	header, err := decoder.ReadRequestHeader()
	if err != nil {
		return err
	}

	// Create response buffer
	var responseBuf bytes.Buffer
	encoder := protocol.NewEncoder(&responseBuf)

	// Route to appropriate handler
	switch header.APIKey {
	case protocol.APIKeyMetadata:
		err = api.HandleMetadata(decoder, encoder, header, s.broker)
	case protocol.APIKeyProduce:
		err = api.HandleProduce(decoder, encoder, header, s.broker)
	case protocol.APIKeyFetch:
		err = api.HandleFetch(decoder, encoder, header, s.broker)
	case protocol.APIKeyListOffsets:
		err = api.HandleListOffsets(decoder, encoder, header, s.broker)
	default:
		log.Printf("Unsupported API key: %d", header.APIKey)
		// Write error response
		if err := encoder.WriteResponseHeader(header.CorrelationID); err != nil {
			return err
		}
		if err := encoder.WriteInt16(protocol.ErrUnsupportedVersion); err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	// Write response size and body
	responseSize := int32(responseBuf.Len())
	sizeBuf := [4]byte{
		byte(responseSize >> 24),
		byte(responseSize >> 16),
		byte(responseSize >> 8),
		byte(responseSize),
	}

	if _, err := conn.Write(sizeBuf[:]); err != nil {
		return err
	}

	if _, err := conn.Write(responseBuf.Bytes()); err != nil {
		return err
	}

	return nil
}
