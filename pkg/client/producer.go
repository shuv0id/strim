package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shuv0id/strim/internal/protocol"
)

var correlationID atomic.Uint32

type Message struct {
	Topic          string
	PartitionIndex int
	Key            []byte
	Value          []byte
	Timestamps     time.Time
	Headers        map[string]string
	MessageResult  *MessageResult
}

// MessageResult contains the outcome of a message produce operation.
type MessageResult struct {
	Offset int   // Kafka offset where the message was stored (-1 if Error is non-nil)
	Error  error // Non-nil if the produce operation failed
}

// pendingRequests holds ProduceRequests that are waiting for a response,
// keyed by correlation ID. Safe for concurrent access.
type pendingRequests struct {
	pendings map[uint32]*protocol.ProduceRequest
	mu       sync.Mutex
}

var pendingReqs = &pendingRequests{
	pendings: make(map[uint32]*protocol.ProduceRequest),
}

type ProducerOpt func(p *ProducerClient) error

type ProducerClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	conn net.Conn // conn represents connection with broker

	batching     bool
	batchSize    int
	batchChan    chan *Message
	batchTimeout time.Duration

	resChan chan *Message
}

func NewProducer(ctx context.Context, addr string, opts ...ProducerOpt) (*ProducerClient, error) {
	ctx, cancel := context.WithCancel(ctx)
	p := &ProducerClient{
		ctx:    ctx,
		cancel: cancel,
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial broker: %v", err)
	}

	p.conn = conn

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	p.wg.Add(1)
	go p.read()

	return p, nil
}

// WithBatch enables message batching. Batches are flushed when the
// batchSize or timeout is reached, whichever occurs first
func WithBatch(batchSize int, timeout time.Duration) ProducerOpt {
	return func(p *ProducerClient) error {
		chanSize := 1000
		if batchSize <= 0 {
			return fmt.Errorf("batch size cannot be <= 0")
		}
		if timeout <= 0 {
			return fmt.Errorf("timeout cannot be <= 0")
		}

		p.batchSize = batchSize
		p.batchTimeout = timeout

		p.batchChan = make(chan *Message, chanSize)
		p.resChan = make(chan *Message, chanSize)

		if p.batching {
			p.wg.Add(1)
			go p.startBatch()
		}

		return nil
	}
}

func (p *ProducerClient) Send(msg *Message) error {
	if msg.Topic == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if msg.PartitionIndex < 0 {
		return fmt.Errorf("partition index cannot be less than 0")
	}

	const maxRetries = 3
	backoff := 100 * time.Millisecond

	if !p.batching {
		for retry := range maxRetries {
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			default:
				var msgs []*Message
				msgs = append(msgs, msg)

				req := convertMsgsToProduceRequest(msgs)

				if err := p.write(req); err != nil {
					return err
				}

				if retry < maxRetries-1 {
					time.Sleep(backoff)
					backoff *= 2
					continue
				}
				return fmt.Errorf("failed to send after %d retries: channel full", maxRetries)
			}
		}

	} else {
		for retry := range maxRetries {
			select {
			case p.batchChan <- msg:
				return nil
			case <-p.ctx.Done():
				return p.ctx.Err()
			default:
				if retry < maxRetries-1 {
					time.Sleep(backoff)
					backoff *= 2
					continue
				}
				return fmt.Errorf("failed to send after %d retries: channel full", maxRetries)
			}
		}

	}
	return nil
}

func (p *ProducerClient) startBatch() {
	defer p.wg.Done()

	batch := make([]*Message, 0, p.batchSize)

	timer := time.NewTimer(p.batchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-p.ctx.Done():
			if len(batch) > 0 {
				if err := p.flush(batch); err != nil {
					log.Printf("[ERR] error flushing batch: %v", err)
				}
			}
			return
		case msg := <-p.batchChan:
			batch = append(batch, msg)

			if len(batch) >= p.batchSize {
				if err := p.flush(batch); err != nil {
					log.Printf("[ERR] error flushing batch: %v", err)
				}
				batch = batch[:0]

				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(p.batchTimeout)
			}
		case <-timer.C:
			if len(batch) > 0 {
				if err := p.flush(batch); err != nil {
					log.Printf("[ERR] error flushing batch: %v", err)
				}
				batch = batch[:0]
			}

			timer.Reset(p.batchTimeout)
		}
	}

}

func (p *ProducerClient) flush(batch []*Message) error {
	if len(batch) == 0 {
		return nil
	}
	req := convertMsgsToProduceRequest(batch)

	if err := p.write(req); err != nil {
		return err
	}

	registerPendingRequest(req)

	return nil
}

func (p *ProducerClient) write(req *protocol.ProduceRequest) error {
	cid := correlationID.Load()
	header := &protocol.RequestHeader{
		MessageType:   protocol.MSG_TYPE_PRODUCE,
		CorrelationID: cid,
	}

	headerDat, err := header.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing request header: %v", err)
	}

	reqData, err := req.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing produce request: %v", err)
	}
	reqSize := len(reqData)

	buf := &bytes.Buffer{}
	err = binary.Write(buf, binary.BigEndian, uint32(reqSize))
	if err != nil {
		return err
	}
	if _, err := buf.Write(headerDat); err != nil {
		return err
	}
	if _, err := buf.Write(reqData); err != nil {
		return err
	}

	if _, err := p.conn.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func registerPendingRequest(req *protocol.ProduceRequest) {
	cid := correlationID.Load()

	pendingReqs.mu.Lock()
	pendingReqs.pendings[cid] = req
	pendingReqs.mu.Unlock()

	correlationID.Add(1)
}

func convertMessageToProduceMessage(msg *Message) *protocol.ProduceMessage {
	timestamp := msg.Timestamps

	if timestamp.IsZero() {
		msg.Timestamps = time.Now()
	}

	return &protocol.ProduceMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: timestamp.UnixMilli(),
		Headers:   msg.Headers,
	}
}

func convertMsgsToProduceRequest(msgs []*Message) *protocol.ProduceRequest {
	if len(msgs) == 0 {
		return &protocol.ProduceRequest{Topics: nil}
	}
	entries := make(map[string]map[int][]*protocol.ProduceMessage) // map entries of topic name to partition index to messages

	for _, msg := range msgs {
		topicName := msg.Topic
		partitionIndex := msg.PartitionIndex

		if _, ok := entries[topicName]; !ok {
			entries[topicName] = make(map[int][]*protocol.ProduceMessage)
		}
		produceMsg := convertMessageToProduceMessage(msg)
		entries[topicName][partitionIndex] = append(entries[topicName][partitionIndex], produceMsg)
	}

	topics := make([]*protocol.TopicData, 0, len(entries))
	for topicName, partitionMap := range entries {
		partitions := make([]*protocol.PartitionData, 0, len(partitionMap))

		for partitionIndex, msgs := range partitionMap {
			partitionData := &protocol.PartitionData{
				Partition: uint32(partitionIndex),
				Messages:  msgs,
			}
			partitions = append(partitions, partitionData)
		}

		topicData := &protocol.TopicData{
			Topic:      topicName,
			Partitions: partitions,
		}
		topics = append(topics, topicData)
	}

	req := &protocol.ProduceRequest{Topics: topics}
	return req
}

// Results return a read-only channel of produced messages.
// Each messaage includes the original data with its MessageResult.
// If the channel is not consumed than the results are dropped silently.
func (p *ProducerClient) Results() <-chan *Message {
	return p.resChan
}

func (p *ProducerClient) read() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			respHeader, resp, err := p.readResponse()
			if err != nil {
				fmt.Printf("Error reading response: %v\n", err)
				return
			}

			if respHeader.MessageType != protocol.MSG_TYPE_PRODUCE {
				log.Printf("wrong message type returned by the broker: %v", respHeader.MessageType)
				continue
			}

			var pendingReq *protocol.ProduceRequest

			pendingReqs.mu.Lock()
			req, ok := pendingReqs.pendings[respHeader.CorrelationID]
			if ok {
				pendingReq = req
			} else {
				log.Printf("no corresponding pending request found with correlationID: %d found for the incoming response: %v", respHeader.CorrelationID, resp)
				continue
			}
			delete(pendingReqs.pendings, respHeader.CorrelationID)
			pendingReqs.mu.Unlock()

			msgs := convertProduceResponseToMsgBatch(pendingReq, resp)
			for _, msg := range msgs {
				select {
				case p.resChan <- msg:
				case <-p.ctx.Done():
					return
				default:
					// Dropping messages in case resChan is consumed slowly or not being consumed.
				}

			}
		}
	}
}

func (p *ProducerClient) readResponse() (*protocol.ResponseHeader, *protocol.ProduceResponse, error) {
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(p.conn, sizeBuf); err != nil {
		return nil, nil, fmt.Errorf("failed to read response size: %v", err)
	}

	responseSize := binary.BigEndian.Uint32(sizeBuf)
	if responseSize == 0 {
		return nil, nil, fmt.Errorf("invalid response size: 0")
	}

	headerBuf := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(p.conn, headerBuf); err != nil {
		return nil, nil, fmt.Errorf("failed to read response header: %v", err)
	}

	header, err := protocol.DeserializeResponseHeader(headerBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize response header: %v", err)
	}

	responseBuf := make([]byte, responseSize)
	if _, err := io.ReadFull(p.conn, responseBuf); err != nil {
		return nil, nil, fmt.Errorf("failed to read response data: %v", err)
	}

	resp, err := protocol.DeserializeProduceResponse(responseBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize response: %v", err)
	}

	return header, resp, nil
}

func convertProduceResponseToMsgBatch(pendingReq *protocol.ProduceRequest, resp *protocol.ProduceResponse) []*Message {
	msgBatch := make(map[string][]*Message)

	for _, topic := range pendingReq.Topics {
		for _, partition := range topic.Partitions {
			topicName := topic.Topic
			partitionIndex := partition.Partition
			key := fmt.Sprintf("%s-%d", topicName, partitionIndex)

			for _, produceMsg := range partition.Messages {
				msg := &Message{
					Topic:          topicName,
					PartitionIndex: int(partitionIndex),
					Key:            produceMsg.Key,
					Value:          produceMsg.Value,
					Timestamps:     time.Unix(0, produceMsg.Timestamp*1000000), // produceMsg.Timestamp in UnixMill -> time.Duration in UnixNano
					Headers:        produceMsg.Headers,
				}
				msgBatch[key] = append(msgBatch[key], msg)
			}
		}
	}

	for _, topicResponse := range resp.Responses {
		for _, partition := range topicResponse.Partitions {
			topicName := topicResponse.Topic
			partitionIndex := partition.Index
			key := fmt.Sprintf("%s-%d", topicName, partitionIndex)

			baseOffset := partition.BaseOffset
			if msgs, ok := msgBatch[key]; ok {
				for i, msg := range msgs {
					msg.MessageResult = &MessageResult{
						Offset: int(baseOffset) + i,
						Error:  nil,
					}
				}

				for _, err := range partition.Errors {
					if int(err.BatchIndex) < len(msgs) {
						msgs[err.BatchIndex].MessageResult = &MessageResult{
							Offset: -1,
							Error:  errors.New(err.ErrorMsg),
						}
					}
				}
			}

		}

	}

	var msgs []*Message

	for _, msg := range msgBatch {
		msgs = append(msg, msg...)
	}

	return msgs
}

// Close gracefully shuts down the producer, cancels internal context,
// waits for goroutines, closes internal channels, and finally closes the broker connection.
func (p *ProducerClient) Close() error {
	p.cancel()

	p.wg.Wait()

	close(p.batchChan)
	close(p.resChan)

	if err := p.conn.Close(); err != nil {
		return fmt.Errorf("error closing connection to broker: %v", err)
	}
	return nil
}
