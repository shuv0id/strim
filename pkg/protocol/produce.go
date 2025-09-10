package protocol

type ProduceMessage struct {
	Key       []byte
	Value     []byte
	Timestamp int64 // Unix millis since epoch
	Headers   map[string]string
}

type RequestHeader struct {
	MessageType   MsgType
	CorrelationID uint32
}

type ResponseHeader struct {
	MessageType   MsgType
	CorrelationID uint32
}

type ProduceRequest struct {
	Topic     string
	Partition uint32
	Messages  []*ProduceMessage
}

type ProduceResponse struct {
	Topic     string
	Partition uint32
	Offset    uint64
	Error     string
}
