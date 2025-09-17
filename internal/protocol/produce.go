package protocol

const HeaderSize = 6 // size(MessageType) + size(CorrelationID)

// Headers
type RequestHeader struct {
	MessageType   MsgType
	CorrelationID uint32
}

type ResponseHeader struct {
	MessageType   MsgType
	CorrelationID uint32
}

// Request
type ProduceMessage struct {
	Key       []byte
	Value     []byte
	Timestamp int64 // Unix millis since epoch
	Headers   map[string]string
}

type PartitionData struct {
	Partition uint32
	Messages  []*ProduceMessage
}

type TopicData struct {
	Topic      string
	Partitions []*PartitionData
}

type ProduceRequest struct {
	Topics []*TopicData
}

// Response
type MessageError struct {
	BatchIndex uint32
	ErrorMsg   string
}

type PartitionResponse struct {
	Index      uint32
	BaseOffset uint64
	Errors     []*MessageError
}

type TopicResponse struct {
	Topic      string
	Partitions []*PartitionResponse
}

type ProduceResponse struct {
	Responses []*TopicResponse
}
