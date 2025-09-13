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

func (pd *PartitionData) serialize(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, pd.Partition)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, uint32(len(pd.Messages)))
	if err != nil {
		return err
	}

	for _, msg := range pd.Messages {
		err = binary.Write(buf, binary.BigEndian, uint32(len(msg.Key)))
		if err != nil {
			return err
		}
		_, err = buf.Write([]byte(msg.Key))
		if err != nil {
			return err
		}

		err = binary.Write(buf, binary.BigEndian, uint32(len(msg.Value)))
		if err != nil {
			return err
		}
		_, err = buf.Write([]byte(msg.Value))
		if err != nil {
			return err
		}

		err = binary.Write(buf, binary.BigEndian, msg.Timestamp)
		if err != nil {
			return err
		}

		err = binary.Write(buf, binary.BigEndian, uint32(len(msg.Headers)))
		if err != nil {
			return err
		}

		if len(msg.Headers) != 0 {
			for k, v := range msg.Headers {
				err = binary.Write(buf, binary.BigEndian, uint32(len(k)))
				if err != nil {
					return err
				}
				_, err = buf.Write([]byte(k))
				if err != nil {
					return err
				}

				err = binary.Write(buf, binary.BigEndian, uint32(len(v)))
				if err != nil {
					return err
				}
				_, err = buf.Write([]byte(v))
				if err != nil {
					return err
				}

			}
		}
	}
	return nil
}

func (td *TopicData) serialize(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, uint32(len(td.Topic)))
	if err != nil {
		return err
	}
	_, err = buf.Write([]byte(td.Topic))
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, uint32(len(td.Partitions)))
	if err != nil {
		return err
	}
	for _, p := range td.Partitions {
		if err := p.serialize(buf); err != nil {
			return err
		}
	}

	return nil
}

func (req *ProduceRequest) Serialize() ([]byte, error) {
	buf := &bytes.Buffer{}
	var err error

	err = binary.Write(buf, binary.BigEndian, uint32(len(req.Topics)))
	if err != nil {
		return nil, err
	}
	for _, t := range req.Topics {
		if err := t.serialize(buf); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func deserializePartitionData(reader *bytes.Reader) (*PartitionData, error) {
	var err error

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

	return &PartitionData{
		Partition: partition,
		Messages:  msgs,
	}, nil
}

func deserializeTopicData(reader *bytes.Reader) (*TopicData, error) {
	var err error

	var topicLen uint32
	if err = binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topic := make([]byte, topicLen)
	if _, err = reader.Read(topic); err != nil {
		return nil, err
	}

	var partitionCount uint32
	if err = binary.Read(reader, binary.BigEndian, &partitionCount); err != nil {
		return nil, err
	}

	var partitions []*PartitionData
	for range partitionCount {
		partionData, err := deserializePartitionData(reader)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partionData)
	}
	return &TopicData{
		Topic:      string(topic),
		Partitions: partitions,
	}, nil
}

func DeserializeProduceRequest(data []byte) (*ProduceRequest, error) {
	reader := bytes.NewReader(data)

	var topicCount uint32
	if err := binary.Read(reader, binary.BigEndian, &topicCount); err != nil {
		return nil, err
	}

	var topics []*TopicData
	for range topicCount {
		topic, err := deserializeTopicData(reader)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
	}

	return &ProduceRequest{
		Topics: topics,
	}, nil
}

func (msgErr *MessageError) serialize(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, msgErr.BatchIndex)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, uint32(len(msgErr.ErrorMsg)))
	if err != nil {
		return err
	}

	buf.Write([]byte(msgErr.ErrorMsg))

	return nil
}

func (pr *PartitionResponse) serialize(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, pr.Index)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, pr.BaseOffset)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, uint32(len(pr.Errors)))
	if err != nil {
		return err
	}
	for _, err := range pr.Errors {
		if err := err.serialize(buf); err != nil {
			return err
		}
	}

	return nil
}

func (tr *TopicResponse) serialize(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, uint32(len(tr.Topic)))
	if err != nil {
		return err
	}

	_, err = buf.Write([]byte(tr.Topic))
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, uint32(len(tr.Partitions)))
	if err != nil {
		return err
	}
	for _, p := range tr.Partitions {
		if err := p.serialize(buf); err != nil {
			return err
		}
	}

	return nil
}

func (resp *ProduceResponse) Serialize() ([]byte, error) {
	buf := &bytes.Buffer{}
	var err error

	err = binary.Write(buf, binary.BigEndian, uint32(len(resp.Responses)))
	if err != nil {
		return nil, err
	}
	for _, resp := range resp.Responses {
		if err := resp.serialize(buf); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func deserializeMessageError(reader *bytes.Reader) (*MessageError, error) {
	var err error

	var batchIndex uint32
	if err = binary.Read(reader, binary.BigEndian, &batchIndex); err != nil {
		return nil, err
	}

	var errMsgLen uint32
	if err := binary.Read(reader, binary.BigEndian, &errMsgLen); err != nil {
		return nil, err
	}
	errMsg := make([]byte, errMsgLen)
	if _, err = reader.Read(errMsg); err != nil {
		return nil, err
	}

	return &MessageError{
		BatchIndex: batchIndex,
		ErrorMsg:   string(errMsg),
	}, nil
}

func deserializePartitionResponse(reader *bytes.Reader) (*PartitionResponse, error) {
	var err error

	var index uint32
	if err = binary.Read(reader, binary.BigEndian, &index); err != nil {
		return nil, err
	}

	var baseOffset uint64
	if err := binary.Read(reader, binary.BigEndian, &baseOffset); err != nil {
		return nil, err
	}

	var errorsCount uint32
	if err := binary.Read(reader, binary.BigEndian, &errorsCount); err != nil {
		return nil, err
	}
	var errs []*MessageError
	for range errorsCount {
		msgErr, err := deserializeMessageError(reader)
		if err != nil {
			return nil, err
		}
		errs = append(errs, msgErr)
	}

	return &PartitionResponse{
		Index:      index,
		BaseOffset: baseOffset,
		Errors:     errs,
	}, nil
}

func deserializeTopicResponse(reader *bytes.Reader) (*TopicResponse, error) {
	var err error

	var topicLen uint32
	if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topic := make([]byte, topicLen)
	if _, err = reader.Read(topic); err != nil {
		return nil, err
	}

	var partitionRespCount uint32
	if err := binary.Read(reader, binary.BigEndian, &partitionRespCount); err != nil {
		return nil, err
	}
	var partitions []*PartitionResponse
	for range partitionRespCount {
		partitionResp, err := deserializePartitionResponse(reader)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partitionResp)
	}

	return &TopicResponse{
		Topic:      string(topic),
		Partitions: partitions,
	}, nil
}

func DeserializeProduceResponse(data []byte) (*ProduceResponse, error) {
	reader := bytes.NewReader(data)
	var err error

	var respCount uint32
	if err = binary.Read(reader, binary.BigEndian, &respCount); err != nil {
		return nil, err
	}

	var resps []*TopicResponse
	for range respCount {
		topicResp, err := deserializeTopicResponse(reader)
		if err != nil {
			return nil, err
		}
		resps = append(resps, topicResp)
	}

	return &ProduceResponse{
		Responses: resps,
	}, nil
}
