package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/shuv0id/strim/internal/protocol"
)

type Server struct {
	addr           string
	listener       net.Listener
	connectionPool map[string]net.Conn
	topicManager   TopicManager
	mu             sync.Mutex
}

func NewServer(address string, topicMngr TopicManager) *Server {
	return &Server{
		addr:           address,
		connectionPool: make(map[string]net.Conn),
		topicManager:   topicMngr,
	}
}

func (server *Server) Start() error {
	listner, err := net.Listen("tcp", server.addr)
	if err != nil {
		return err
	}

	server.listener = listner
	server.addr = listner.Addr().String()

	return nil
}

func (server *Server) Serve(ctx context.Context) error {
	if server.listener == nil {
		return fmt.Errorf("listener not started")
	}
	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			conn, err := server.listener.Accept()
			if err != nil {
				return err
			}
			fmt.Println("Connection established with: ", conn.RemoteAddr().String())

			addr := conn.RemoteAddr().String()
			server.mu.Lock()
			server.connectionPool[addr] = conn
			server.mu.Unlock()

			go server.handleConnection(ctx, conn)
		}
	}
}

func (server *Server) handleConnection(ctx context.Context, conn net.Conn) {
	lengthBuf := make([]byte, 4)
	headerBuf := make([]byte, protocol.HeaderSize)
	msgBuf := make([]byte, 8192)

readLoop:
	for {
		fmt.Println("hello")
		select {
		case <-ctx.Done():
			if err := conn.Close(); err != nil {
				log.Printf("[ERR] error closing connection %v", err)
			}
			break readLoop
		default:
			_, err := io.ReadFull(conn, lengthBuf)
			if err != nil {
				log.Printf("error reading length: %v\n", err)
				break readLoop
			}

			msgSize := binary.BigEndian.Uint32(lengthBuf)

			_, err = io.ReadFull(conn, headerBuf)
			if err != nil {
				log.Printf("error reading request header: %v\n", err)
				break readLoop
			}

			h, err := protocol.DeserializeRequestHeader(headerBuf)
			if err != nil {
				log.Printf("error deserialising request header: %v\n", err)
				break readLoop
			}

			if len(msgBuf) < int(msgSize) {
				msgBuf = make([]byte, msgSize)
			} else {
				msgBuf = msgBuf[:msgSize]
			}

			_, err = io.ReadFull(conn, msgBuf)
			if err != nil {
				log.Printf("error reading request body: %v\n", err)
				break readLoop
			}

			if err := server.processMsg(conn, h.MessageType, h.CorrelationID, msgBuf); err != nil {
				log.Printf("error processing request body: %v\n", err)
				break readLoop
			}

		}
	}
}

func (server *Server) processMsg(conn net.Conn, msgType protocol.MsgType, correlationID uint32, msgDat []byte) error {
	switch msgType {
	case protocol.MSG_TYPE_PRODUCE:
		req, err := protocol.DeserializeProduceRequest(msgDat)
		if err != nil {
			return err
		}

		if err := server.handleProduceRequest(conn, correlationID, req); err != nil {
			return err
		}
	}

	return nil
}

func (server *Server) Stop() error {
	err := server.listener.Close()
	return err
}
