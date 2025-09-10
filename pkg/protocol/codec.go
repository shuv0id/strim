package protocol

import (
	"bytes"
	"encoding/binary"
)

func (h *RequestHeader) Serialize() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, h.MessageType); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, h.CorrelationID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeRequestHeader(data []byte) (*RequestHeader, error) {
	reader := bytes.NewReader(data)
	var msgType MsgType
	if err := binary.Read(reader, binary.BigEndian, &msgType); err != nil {
		return nil, err
	}
	var correlationID uint32
	if err := binary.Read(reader, binary.BigEndian, &correlationID); err != nil {
		return nil, err
	}
	return &RequestHeader{
		MessageType:   msgType,
		CorrelationID: correlationID,
	}, nil
}

func (h *ResponseHeader) Serialize() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, h.MessageType); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, h.CorrelationID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeResponseHeader(data []byte) (*ResponseHeader, error) {
	reader := bytes.NewReader(data)
	var msgType MsgType
	if err := binary.Read(reader, binary.BigEndian, &msgType); err != nil {
		return nil, err
	}
	var correlationID uint32
	if err := binary.Read(reader, binary.BigEndian, &correlationID); err != nil {
		return nil, err
	}
	return &ResponseHeader{
		MessageType:   msgType,
		CorrelationID: correlationID,
	}, nil
}

func (req *ProduceRequest) Serialize() ([]byte, error) {
	buf := &bytes.Buffer{}
	var err error

	err = binary.Write(buf, binary.BigEndian, uint32(len(req.Topic)))
	if err != nil {
		return nil, err
	}

	_, err = buf.Write([]byte(req.Topic))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, req.Partition)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, uint32(len(req.Messages)))
	if err != nil {
		return nil, err
	}

	for _, msg := range req.Messages {
		err = binary.Write(buf, binary.BigEndian, uint32(len(msg.Key)))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write([]byte(msg.Key))
		if err != nil {
			return nil, err
		}

		err = binary.Write(buf, binary.BigEndian, uint32(len(msg.Value)))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write([]byte(msg.Value))
		if err != nil {
			return nil, err
		}

		err = binary.Write(buf, binary.BigEndian, msg.Timestamp)
		if err != nil {
			return nil, err
		}

		err = binary.Write(buf, binary.BigEndian, uint32(len(msg.Headers)))
		if err != nil {
			return nil, err
		}

		if len(msg.Headers) != 0 {
			for k, v := range msg.Headers {
				err = binary.Write(buf, binary.BigEndian, uint32(len(k)))
				if err != nil {
					return nil, err
				}
				_, err = buf.Write([]byte(k))
				if err != nil {
					return nil, err
				}

				err = binary.Write(buf, binary.BigEndian, uint32(len(v)))
				if err != nil {
					return nil, err
				}
				_, err = buf.Write([]byte(v))
				if err != nil {
					return nil, err
				}

			}
		}

	}

	return buf.Bytes(), nil
}

func DeserializeProduceRequest(data []byte) (*ProduceRequest, error) {
	reader := bytes.NewReader(data)
	var err error

	var topicLen uint32
	if err = binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}

	topic := make([]byte, topicLen)
	if _, err = reader.Read(topic); err != nil {
		return nil, err
	}

	var partition uint32
	if err = binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	var msgCount uint32
	if err = binary.Read(reader, binary.BigEndian, &msgCount); err != nil {
		return nil, err
	}

	var msgs []*ProduceMessage
	for range msgCount {
		msg := &ProduceMessage{}

		var keyLen uint32
		if err = binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err = reader.Read(key); err != nil {
			return nil, err
		}
		msg.Key = key

		var valueLen uint32
		if err = binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}
		value := make([]byte, valueLen)
		if _, err = reader.Read(value); err != nil {
			return nil, err
		}
		msg.Value = value

		var timestamp uint64
		if err = binary.Read(reader, binary.BigEndian, &timestamp); err != nil {
			return nil, err
		}
		msg.Timestamp = int64(timestamp)

		var headersCount uint32
		if err = binary.Read(reader, binary.BigEndian, &headersCount); err != nil {
			return nil, err
		}

		msg.Headers = make(map[string]string)
		for range headersCount {
			var kLen uint32
			if err = binary.Read(reader, binary.BigEndian, &kLen); err != nil {
				return nil, err
			}
			k := make([]byte, kLen)
			if _, err = reader.Read(k); err != nil {
				return nil, err
			}

			var vLen uint32
			if err = binary.Read(reader, binary.BigEndian, &vLen); err != nil {
				return nil, err
			}
			v := make([]byte, vLen)
			if _, err = reader.Read(v); err != nil {
				return nil, err
			}

			msg.Headers[string(k)] = string(v)
		}

		msgs = append(msgs, msg)
	}

	return &ProduceRequest{
		Topic:     string(topic),
		Partition: partition,
		Messages:  msgs,
	}, nil
}

func (response *ProduceResponse) Serialize() ([]byte, error) {
	buf := &bytes.Buffer{}
	var err error

	err = binary.Write(buf, binary.BigEndian, uint32(len(response.Topic)))
	if err != nil {
		return nil, err
	}
	_, err = buf.Write([]byte(response.Topic))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, response.Partition)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, response.Offset)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, uint32(len(response.Error)))
	if err != nil {
		return nil, err
	}
	_, err = buf.Write([]byte(response.Error))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DeserializeProduceResponse(data []byte) (*ProduceResponse, error) {
	reader := bytes.NewReader(data)
	var err error

	var topicLen uint32
	if err = binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topic := make([]byte, topicLen)
	if _, err = reader.Read(topic); err != nil {
		return nil, err
	}

	var partition uint32
	if err = binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	var offset uint64
	if err = binary.Read(reader, binary.BigEndian, &offset); err != nil {
		return nil, err
	}

	var errLen uint32
	if err = binary.Read(reader, binary.BigEndian, &errLen); err != nil {
		return nil, err
	}

	var errorMsg []byte
	if errLen > 0 {
		errorMsg = make([]byte, errLen)
		if _, err = reader.Read(errorMsg); err != nil {
			return nil, err
		}
	}

	return &ProduceResponse{
		Topic:     string(topic),
		Partition: partition,
		Offset:    offset,
		Error:     string(errorMsg),
	}, nil
}
