package partition

import (
	"encoding/binary"
	"fmt"
)

type Message struct {
	Offset    uint64
	Timestamp int64 // Unix millis since epoch
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"Message{Offset=%d, Timestamp=%d, Topic=%q, Key=%q, Value=%q, Headers=%v}",
		m.Offset,
		m.Timestamp,
		m.Topic,
		string(m.Key),
		string(m.Value),
		m.Headers,
	)
}

// Validate checks whether the Message has all required fields populated.
// It returns an error if any mandatory field (Topic, Key, Value, or Timestamp)
// is missing or invalid.
func (m *Message) Validate() error {
	if m.Topic == "" {
		return fmt.Errorf("message topic cannot be empty")
	}

	if len(m.Key) == 0 {
		return fmt.Errorf("message key cannot be empty")
	}

	if len(m.Value) == 0 {
		return fmt.Errorf("message value cannot be empty")
	}

	if m.Timestamp <= 0 {
		return fmt.Errorf("message timestamps is invalid")
	}

	if m.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	return nil
}

// Serialize serializes a Message to BigEndian binary format.
// The size is pre-calculated before serialization.
// Serialized data layout:
//
//	offset + timestamp + topicLen + topic +
//	keyLen + key + valueLen + value +
//	headersCount + headers(keyLen + key + valueLen + value + ...)
func (m *Message) Serialize() []byte {
	msgSize := 8 + 8 + 4 + len(m.Topic) + 4 + len(m.Key) + 4 + len(m.Value) + 4 // size(offset) + size(timestamps) + size(topicLen) + size(topic) + size(keyLen) + size(key) + size(valueLen) + size(value) + size(headersCount)

	if len(m.Headers) != 0 {
		for k, v := range m.Headers {
			msgSize += 4 + len([]byte(k)) + 4 + len([]byte(v))
		}
	}

	buf := make([]byte, 0, msgSize)

	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, m.Offset)
	buf = append(buf, offsetBytes...)

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(m.Timestamp))
	buf = append(buf, timestampBytes...)

	buf = appendStrLengthPrefixed(buf, []byte(m.Topic))

	buf = appendStrLengthPrefixed(buf, []byte(m.Key))

	buf = appendStrLengthPrefixed(buf, []byte(m.Value))

	headersCount := make([]byte, 4)
	binary.BigEndian.PutUint32(headersCount, uint32(len(m.Headers)))
	buf = append(buf, headersCount...)

	if len(m.Headers) != 0 {
		for k, v := range m.Headers {
			kLen := make([]byte, 4)
			binary.BigEndian.PutUint32(kLen, uint32(len(k)))
			buf = append(buf, kLen...)
			buf = append(buf, k...)

			vLen := make([]byte, 4)
			binary.BigEndian.PutUint32(vLen, uint32(len(v)))
			buf = append(buf, vLen...)
			buf = append(buf, v...)
		}
	}

	return buf
}

// Deserialize decodes a serialized message body from msgBytes (big-endian encoded fields)
// and returns a Message. Returns an error if msgBytes does not contain enough
// data to fully decode the Message.
func Deserialize(msgBytes []byte) (*Message, error) {
	m := &Message{}
	pos := 0

	if len(msgBytes) < pos+8 {
		return nil, fmt.Errorf("not enough data for offset")
	}
	m.Offset = binary.BigEndian.Uint64(msgBytes[pos:8])
	pos += 8

	if len(msgBytes) < pos+8 {
		return nil, fmt.Errorf("not enough data for timestamp")
	}
	m.Timestamp = int64(binary.BigEndian.Uint64(msgBytes[pos : pos+8]))
	pos += 8

	if len(msgBytes) < pos+4 {
		return nil, fmt.Errorf("not enough data for topic length")
	}
	topicLen := binary.BigEndian.Uint32(msgBytes[pos : pos+4])
	pos += 4
	if len(msgBytes) < pos+int(topicLen) {
		return nil, fmt.Errorf("not enough data for topic")
	}
	m.Topic = string(msgBytes[pos : pos+int(topicLen)])
	pos += int(topicLen)

	if len(msgBytes) < pos+4 {
		return nil, fmt.Errorf("not enough data for key length")
	}
	keyLen := binary.BigEndian.Uint32(msgBytes[pos : pos+4])
	pos += 4
	if len(msgBytes) < pos+int(keyLen) {
		return nil, fmt.Errorf("not enough data for key")
	}
	m.Key = msgBytes[pos : pos+int(keyLen)]
	pos += int(keyLen)

	if len(msgBytes) < pos+4 {
		return nil, fmt.Errorf("not enough data for value length")
	}
	valLen := binary.BigEndian.Uint32(msgBytes[pos : pos+4])
	pos += 4
	if len(msgBytes) < pos+int(valLen) {
		return nil, fmt.Errorf("not enough data for value")
	}
	m.Value = msgBytes[pos : pos+int(valLen)]
	pos += int(valLen)

	if len(msgBytes) < pos+4 {
		return nil, fmt.Errorf("not enough data for headers count")
	}
	headersCount := binary.BigEndian.Uint32(msgBytes[pos : pos+4])
	pos += 4
	m.Headers = make(map[string]string)

	for i := 0; i < int(headersCount); i++ {
		if len(msgBytes) < pos+4 {
			return nil, fmt.Errorf("not enough data for header key length")
		}
		kLen := binary.BigEndian.Uint32(msgBytes[pos : pos+4])
		pos += 4
		if len(msgBytes) < pos+int(kLen) {
			return nil, fmt.Errorf("not enough data for header key")
		}
		key := string(msgBytes[pos : pos+int(kLen)])
		pos += int(kLen)

		if len(msgBytes) < pos+4 {
			return nil, fmt.Errorf("not enough data for header value length")
		}
		vLen := binary.BigEndian.Uint32(msgBytes[pos : pos+4])
		pos += 4
		if len(msgBytes) < pos+int(vLen) {
			return nil, fmt.Errorf("not enough data for header value")
		}
		val := string(msgBytes[pos : pos+int(vLen)])
		pos += int(vLen)

		m.Headers[key] = val
	}

	return m, nil
}

func appendStrLengthPrefixed(buf []byte, data []byte) []byte {
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(data)))
	buf = append(buf, lengthBytes...)
	buf = append(buf, data...)
	return buf
}
