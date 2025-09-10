package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestHeader_RoundTrip(t *testing.T) {
	testCases := []struct {
		name   string
		header *RequestHeader
	}{
		{
			name: "produce message",
			header: &RequestHeader{
				MessageType:   MSG_TYPE_PRODUCE,
				CorrelationID: 12345,
			},
		},
		{
			name: "zero correlation id",
			header: &RequestHeader{
				MessageType:   MSG_TYPE_PRODUCE,
				CorrelationID: 0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.header.Serialize()
			require.NoError(t, err, "serialize should not fail")
			require.NotEmpty(t, data, "serialized data should not be empty")

			deserialized, err := DeserializeRequestHeader(data)
			require.NoError(t, err, "deserialize should not fail")
			require.NotNil(t, deserialized, "deserialized header should not be nil")

			assert.Equal(t, tc.header.MessageType, deserialized.MessageType)
			assert.Equal(t, tc.header.CorrelationID, deserialized.CorrelationID)
		})
	}
}

func TestResponseHeader_RoundTrip(t *testing.T) {
	testCases := []struct {
		name   string
		header *ResponseHeader
	}{
		{
			name: "produce response",
			header: &ResponseHeader{
				MessageType:   MSG_TYPE_PRODUCE,
				CorrelationID: 12345,
			},
		},
		{
			name: "zero correlation id",
			header: &ResponseHeader{
				MessageType:   MSG_TYPE_PRODUCE,
				CorrelationID: 0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.header.Serialize()
			require.NoError(t, err, "serialize should not fail")
			require.NotEmpty(t, data, "serialized data should not be empty")

			deserialized, err := DeserializeResponseHeader(data)
			require.NoError(t, err, "deserialize should not fail")
			require.NotNil(t, deserialized, "deserialized header should not be nil")

			assert.Equal(t, tc.header.MessageType, deserialized.MessageType)
			assert.Equal(t, tc.header.CorrelationID, deserialized.CorrelationID)
		})
	}
}

func TestProduceRequest_RoundTrip(t *testing.T) {
	msg1 := &ProduceMessage{
		Key:       []byte("k1"),
		Value:     []byte("v1"),
		Timestamp: time.Now().UnixMilli(),
		Headers: map[string]string{
			"version":      "1.2.3",
			"content-type": "application/json",
		},
	}
	msg2 := &ProduceMessage{
		Key:       []byte("k2"),
		Value:     []byte("v2"),
		Timestamp: time.Now().UnixMilli(),
		Headers:   nil,
	}
	msg3 := &ProduceMessage{
		Key:       []byte("k3"),
		Value:     []byte("v3"),
		Timestamp: time.Now().UnixMilli(),
	}

	testCases := []struct {
		name string
		req  *ProduceRequest
	}{
		{
			"Single message",
			&ProduceRequest{
				Topic: "topicA", Partition: 0, Messages: []*ProduceMessage{msg1},
			},
		},
		{
			"Multiple messages",
			&ProduceRequest{
				Topic: "topicB", Partition: 2, Messages: []*ProduceMessage{msg1, msg2, msg3},
			},
		},
		{
			"No messages",
			&ProduceRequest{
				Topic: "topicC", Partition: 1, Messages: nil,
			},
		},
		{
			"empty messages array",
			&ProduceRequest{
				Topic: "events", Partition: 0, Messages: []*ProduceMessage{},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.req.Serialize()
			require.NoError(t, err, "serialize should return nil err")
			require.NotEmpty(t, data, "serialize should not return empty data")

			deserialized, err := DeserializeProduceRequest(data)
			require.NoError(t, err, "deserialize should not fail")

			assert.Equal(t, tc.req.Topic, deserialized.Topic, "topic should match")
			assert.Equal(t, tc.req.Partition, deserialized.Partition, "partition should match")

			for i, produceMsg := range tc.req.Messages {
				assert.Equal(t, produceMsg.Key, deserialized.Messages[i].Key, "produce message key should match")
				assert.Equal(t, produceMsg.Value, deserialized.Messages[i].Value, "produce message value should match")
			}
		})
	}
}

func TestProduceResponse_RoundTrip(t *testing.T) {
	testCases := []struct {
		name string
		resp ProduceResponse
	}{
		{
			name: "successful response",
			resp: ProduceResponse{
				Topic:     "events",
				Partition: 0,
				Offset:    12345,
				Error:     "",
			},
		},
		{
			name: "response with error",
			resp: ProduceResponse{
				Topic:     "events",
				Partition: 0,
				Offset:    12345,
				Error:     "ERROR TOPIC NOT FOUND",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.resp.Serialize()
			require.NoError(t, err, "serialize should return nil err")
			require.NotEmpty(t, data, "serialize should not return empty data")

			deserialized, err := DeserializeProduceResponse(data)
			require.NoErrorf(t, err, "deserialize should not fail %v", err)

			assert.Equal(t, tc.resp.Topic, deserialized.Topic, "topic should match")
			assert.Equal(t, tc.resp.Partition, deserialized.Partition, "partition should match")
			assert.Equal(t, tc.resp.Offset, deserialized.Offset, "offset should match")
			assert.Equal(t, tc.resp.Error, deserialized.Error, "offset should match")

		})
	}
}
