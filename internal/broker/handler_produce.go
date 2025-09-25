package broker

import (
	"bytes"
	"encoding/binary"
	"net"

	"github.com/shuv0id/strim/internal/partition"
	"github.com/shuv0id/strim/internal/protocol"
)

func (s *Server) handleProduceRequest(conn net.Conn, correlationID uint32, produceReq *protocol.ProduceRequest) error {

	produceResp := s.processProduceRequest(produceReq)

	err := s.sendProduceResp(conn, produceResp, correlationID)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) sendProduceResp(conn net.Conn, produceResp *protocol.ProduceResponse, correlationID uint32) error {

	respDat, err := produceResp.Serialize()
	if err != nil {
		// NOTE: will be adding log here
		return err
	}

	h := &protocol.ResponseHeader{
		MessageType:   protocol.MSG_TYPE_PRODUCE,
		CorrelationID: correlationID,
	}

	headerDat, err := h.Serialize()
	if err != nil {
		return err
	}

	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(respDat))); err != nil {
		return err
	}
	if _, err := buf.Write(headerDat); err != nil {
		return err
	}
	if _, err := buf.Write(headerDat); err != nil {
		return err
	}
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) processProduceRequest(req *protocol.ProduceRequest) *protocol.ProduceResponse {
	if len(req.Topics) == 0 {
		return &protocol.ProduceResponse{
			Responses: nil,
		}
	}

	var topicResps []*protocol.TopicResponse
	for _, topic := range req.Topics {
		topicResp := s.processTopicData(topic)
		if topicResp != nil {
			topicResps = append(topicResps, topicResp)
		}
	}

	return &protocol.ProduceResponse{
		Responses: topicResps,
	}
}

func (s *Server) processTopicData(topicData *protocol.TopicData) *protocol.TopicResponse {
	if err := s.ensureTopicExists(topicData.Topic); err != nil {
		return nil
	}

	t := s.topicManager.GetTopic(topicData.Topic)

	if len(topicData.Partitions) == 0 {
		return &protocol.TopicResponse{
			Partitions: nil,
		}
	}

	var partitionResps []*protocol.PartitionResponse
	for _, partitionData := range topicData.Partitions {
		partitionResp := s.processPartitionData(t.Name, partitionData)
		partitionResps = append(partitionResps, partitionResp)
	}

	topicResp := &protocol.TopicResponse{
		Topic:      topicData.Topic,
		Partitions: partitionResps,
	}

	return topicResp
}

func (s *Server) ensureTopicExists(topicName string) error {
	if t := s.topicManager.GetTopic(topicName); t != nil {
		return nil
	}

	partitionCount := s.getDefaultPartitionCount()
	return s.topicManager.CreateTopic(topicName, partitionCount)
}

func (s *Server) getDefaultPartitionCount() int {
	// topicCfg := s.topicManager.GetTopicConfig()
	// if topicCfg == nil || topicCfg.NumPartitions == 0 {
	// 	topicCfg.NumPartitions = 1
	// }
	return 1
}

func (s *Server) processPartitionData(topicName string, partitionData *protocol.PartitionData) *protocol.PartitionResponse {
	p := s.topicManager.GetPartition(topicName, int(partitionData.Partition))
	if p == nil {
		return nil
	}

	if len(partitionData.Messages) == 0 {
		return &protocol.PartitionResponse{
			Index:      p.GetPartitionIndex(),
			BaseOffset: 0,
			Errors:     nil,
			ErrorCode:  -1,
		}
	}

	msgs := convertToPartitionMsgs(topicName, partitionData.Messages, p)

	batchErrs := p.Append(msgs)
	var msgErrs []*protocol.MessageError
	if len(batchErrs.Errors) != 0 {
		msgErrs = convertBatchErrorsToMessageErrors(batchErrs.Errors)
	}

	partitionResp := &protocol.PartitionResponse{
		Index:      p.GetPartitionIndex(),
		BaseOffset: uint64(batchErrs.BaseOffset),
		Errors:     msgErrs,
	}

	return partitionResp
}

func convertToPartitionMsgs(topicName string, produceMsgs []*protocol.ProduceMessage, p partition.Partition) []*partition.Message {
	msgs := make([]*partition.Message, 0, len(produceMsgs))
	baseOffset := p.GetLEO()
	for i, producerMsg := range produceMsgs {
		msg := &partition.Message{
			Offset:    baseOffset + uint64(i),
			Timestamp: producerMsg.Timestamp,
			Topic:     topicName,
			Key:       producerMsg.Key,
			Value:     producerMsg.Value,
			Headers:   producerMsg.Headers,
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func convertBatchErrorsToMessageErrors(batchErrs []*partition.BatchError) []*protocol.MessageError {
	msgErrs := make([]*protocol.MessageError, 0, len(batchErrs))
	for _, err := range batchErrs {
		msgErr := &protocol.MessageError{
			BatchIndex: uint32(err.BatchIndex()),
			ErrorMsg:   err.Error(),
		}
		msgErrs = append(msgErrs, msgErr)
	}

	return msgErrs
}
