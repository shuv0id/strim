package protocol

import (
	"encoding/binary"
	"fmt"
	"time"
)

type Message struct {
	Offset    uint64            `json:"offset"`
	Timestamp time.Time         `json:"timestamp"`
	Topic     string            `json:"topic"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"Value,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

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

	if m.Timestamp.Unix() == 0 {
		return fmt.Errorf("message timestamps is invalid")
	}

	if m.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	return nil
}

func (m *Message) Serialize() []byte {
	buf := make([]byte, 0, 1024)

	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, m.Offset)
	buf = append(buf, offsetBytes...)

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(m.Timestamp.UnixMilli()))
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

func Deserialize(b []byte) (*Message, error) {
	m := &Message{}
	pos := 0

	if len(b) < pos+8 {
		return nil, fmt.Errorf("not enough data for offset")
	}
	m.Offset = binary.BigEndian.Uint64(b[0:8])
	pos += 8

	if len(b) < pos+8 {
		return nil, fmt.Errorf("not enough data for timestamp")
	}
	m.Timestamp = time.UnixMilli(int64(binary.BigEndian.Uint64(b[pos : pos+8])))
	pos += 8

	if len(b) < pos+4 {
		return nil, fmt.Errorf("not enough data for topic length")
	}
	topicLen := binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4
	if len(b) < pos+int(topicLen) {
		return nil, fmt.Errorf("not enough data for topic")
	}
	m.Topic = string(b[pos : pos+int(topicLen)])
	pos += int(topicLen)

	if len(b) < pos+4 {
		return nil, fmt.Errorf("not enough data for key length")
	}
	keyLen := binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4
	if len(b) < pos+int(keyLen) {
		return nil, fmt.Errorf("not enough data for key length")
	}
	m.Key = b[pos : pos+int(keyLen)]
	pos += int(keyLen)

	if len(b) < pos+4 {
		return nil, fmt.Errorf("not enough data for value length")
	}
	valLen := binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4
	if len(b) < pos+int(valLen) {
		return nil, fmt.Errorf("not enough data for key length")
	}
	m.Value = b[pos : pos+int(valLen)]
	pos += int(valLen)

	if len(b) < pos+4 {
		return nil, fmt.Errorf("not enough data for headers count")
	}
	headersCount := binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4
	m.Headers = make(map[string]string)

	for i := 0; i < int(headersCount); i++ {
		if len(b) < pos+4 {
			return nil, fmt.Errorf("not enough data for header key length")
		}
		kLen := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		if len(b) < pos+int(kLen) {
			return nil, fmt.Errorf("not enough data for header key")
		}
		key := string(b[pos : pos+int(kLen)])
		pos += int(kLen)

		if len(b) < pos+4 {
			return nil, fmt.Errorf("not enough data for header value length")
		}
		vLen := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		if len(b) < pos+int(vLen) {
			return nil, fmt.Errorf("not enough data for header value")
		}
		val := string(b[pos : pos+int(vLen)])
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
