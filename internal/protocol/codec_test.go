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
	tests := []struct {
		name    string
		request *ProduceRequest
	}{
		{
			name: "empty request",
			request: &ProduceRequest{
				Topics: []*TopicData{},
			},
		},
		{
			name: "single topic, single partition, single message",
			request: &ProduceRequest{
				Topics: []*TopicData{
					{
						Topic: "test-topic",
						Partitions: []*PartitionData{
							{
								Partition: 0,
								Messages: []*ProduceMessage{
									{
										Key:       []byte("key1"),
										Value:     []byte("value1"),
										Timestamp: time.Now().Unix() * 1000,
										Headers:   map[string]string{},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "single topic, single partition, message with headers",
			request: &ProduceRequest{
				Topics: []*TopicData{
					{
						Topic: "test-topic",
						Partitions: []*PartitionData{
							{
								Partition: 0,
								Messages: []*ProduceMessage{
									{
										Key:       []byte("key1"),
										Value:     []byte("value1"),
										Timestamp: 1640995200000,
										Headers: map[string]string{
											"header1": "value1",
											"header2": "value2",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple topics, partitions, and messages",
			request: &ProduceRequest{
				Topics: []*TopicData{
					{
						Topic: "topic1",
						Partitions: []*PartitionData{
							{
								Partition: 0,
								Messages: []*ProduceMessage{
									{
										Key:       []byte("key1"),
										Value:     []byte("value1"),
										Timestamp: 1640995200000,
										Headers:   map[string]string{"h1": "v1"},
									},
									{
										Key:       []byte("key2"),
										Value:     []byte("value2"),
										Timestamp: 1640995201000,
										Headers:   map[string]string{},
									},
								},
							},
							{
								Partition: 1,
								Messages: []*ProduceMessage{
									{
										Key:       []byte("key3"),
										Value:     []byte("value3"),
										Timestamp: 1640995202000,
										Headers:   map[string]string{"h2": "v2", "h3": "v3"},
									},
								},
							},
						},
					},
					{
						Topic: "topic2",
						Partitions: []*PartitionData{
							{
								Partition: 0,
								Messages: []*ProduceMessage{
									{
										Key:       []byte("key4"),
										Value:     []byte("value4"),
										Timestamp: 1640995203000,
										Headers:   map[string]string{},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "empty keys and values",
			request: &ProduceRequest{
				Topics: []*TopicData{
					{
						Topic: "test-topic",
						Partitions: []*PartitionData{
							{
								Partition: 0,
								Messages: []*ProduceMessage{
									{
										Key:       []byte{},
										Value:     []byte{},
										Timestamp: 1640995200000,
										Headers:   map[string]string{},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.request.Serialize()
			require.NoError(t, err)
			require.NotEmpty(t, data)

			deserialized, err := DeserializeProduceRequest(data)
			require.NoError(t, err)
			require.NotNil(t, deserialized)

			assertProduceRequestEqual(t, tt.request, deserialized)
		})
	}
}

func TestProduceResponse_RoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		response *ProduceResponse
	}{
		{
			name: "empty response",
			response: &ProduceResponse{
				Responses: []*TopicResponse{},
			},
		},
		{
			name: "single topic response with no errors",
			response: &ProduceResponse{
				Responses: []*TopicResponse{
					{
						Topic: "test-topic",
						Partitions: []*PartitionResponse{
							{
								Index:      0,
								BaseOffset: 1000,
								Errors:     []*MessageError{},
							},
						},
					},
				},
			},
		},
		{
			name: "single topic response with errors",
			response: &ProduceResponse{
				Responses: []*TopicResponse{
					{
						Topic: "test-topic",
						Partitions: []*PartitionResponse{
							{
								Index:      0,
								BaseOffset: 1000,
								Errors: []*MessageError{
									{
										BatchIndex: 0,
										ErrorMsg:   "Message too large",
									},
									{
										BatchIndex: 1,
										ErrorMsg:   "Invalid format",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple topics and partitions",
			response: &ProduceResponse{
				Responses: []*TopicResponse{
					{
						Topic: "topic1",
						Partitions: []*PartitionResponse{
							{
								Index:      0,
								BaseOffset: 1000,
								Errors:     []*MessageError{},
							},
							{
								Index:      1,
								BaseOffset: 2000,
								Errors: []*MessageError{
									{
										BatchIndex: 0,
										ErrorMsg:   "Partition offline",
									},
								},
							},
						},
					},
					{
						Topic: "topic2",
						Partitions: []*PartitionResponse{
							{
								Index:      0,
								BaseOffset: 3000,
								Errors:     []*MessageError{},
							},
						},
					},
				},
			},
		},
		{
			name: "max offset values",
			response: &ProduceResponse{
				Responses: []*TopicResponse{
					{
						Topic: "test-topic",
						Partitions: []*PartitionResponse{
							{
								Index:      0xFFFFFFFF,
								BaseOffset: 0xFFFFFFFFFFFFFFFF,
								Errors:     []*MessageError{},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.response.Serialize()
			require.NoError(t, err)
			require.NotEmpty(t, data)

			deserialized, err := DeserializeProduceResponse(data)
			require.NoError(t, err)
			require.NotNil(t, deserialized)

			assertProduceResponseEqual(t, tt.response, deserialized)
		})
	}
}

func assertProduceRequestEqual(t *testing.T, expected, actual *ProduceRequest) {
	require.Equal(t, len(expected.Topics), len(actual.Topics))

	for i, expectedTopic := range expected.Topics {
		actualTopic := actual.Topics[i]
		assert.Equal(t, expectedTopic.Topic, actualTopic.Topic)
		require.Equal(t, len(expectedTopic.Partitions), len(actualTopic.Partitions))

		for j, expectedPartition := range expectedTopic.Partitions {
			actualPartition := actualTopic.Partitions[j]
			assert.Equal(t, expectedPartition.Partition, actualPartition.Partition)
			require.Equal(t, len(expectedPartition.Messages), len(actualPartition.Messages))

			for k, expectedMessage := range expectedPartition.Messages {
				actualMessage := actualPartition.Messages[k]
				assert.Equal(t, expectedMessage.Key, actualMessage.Key)
				assert.Equal(t, expectedMessage.Value, actualMessage.Value)
				assert.Equal(t, expectedMessage.Timestamp, actualMessage.Timestamp)
				assert.Equal(t, expectedMessage.Headers, actualMessage.Headers)
			}
		}
	}
}

func assertProduceResponseEqual(t *testing.T, expected, actual *ProduceResponse) {
	require.Equal(t, len(expected.Responses), len(actual.Responses))

	for i, expectedResponse := range expected.Responses {
		actualResponse := actual.Responses[i]
		assert.Equal(t, expectedResponse.Topic, actualResponse.Topic)
		require.Equal(t, len(expectedResponse.Partitions), len(actualResponse.Partitions))

		for j, expectedPartition := range expectedResponse.Partitions {
			actualPartition := actualResponse.Partitions[j]
			assert.Equal(t, expectedPartition.Index, actualPartition.Index)
			assert.Equal(t, expectedPartition.BaseOffset, actualPartition.BaseOffset)
			require.Equal(t, len(expectedPartition.Errors), len(actualPartition.Errors))

			for k, expectedError := range expectedPartition.Errors {
				actualError := actualPartition.Errors[k]
				assert.Equal(t, expectedError.BatchIndex, actualError.BatchIndex)
				assert.Equal(t, expectedError.ErrorMsg, actualError.ErrorMsg)
			}
		}
	}
}

func TestSerializationEdgeCases(t *testing.T) {
	t.Run("invalid data for request deserialization", func(t *testing.T) {
		_, err := DeserializeProduceRequest([]byte{0x01})
		assert.Error(t, err)
	})

	t.Run("invalid data for response deserialization", func(t *testing.T) {
		_, err := DeserializeProduceResponse([]byte{0x01})
		assert.Error(t, err)
	})

	t.Run("empty data", func(t *testing.T) {
		_, err := DeserializeRequestHeader([]byte{})
		assert.Error(t, err)

		_, err = DeserializeResponseHeader([]byte{})
		assert.Error(t, err)

		_, err = DeserializeProduceRequest([]byte{})
		assert.Error(t, err)

		_, err = DeserializeProduceResponse([]byte{})
		assert.Error(t, err)
	})
}

func BenchmarkProduceRequestRoundTrip(b *testing.B) {
	request := &ProduceRequest{
		Topics: []*TopicData{
			{
				Topic: "benchmark-topic",
				Partitions: []*PartitionData{
					{
						Partition: 0,
						Messages: []*ProduceMessage{
							{
								Key:       []byte("benchmark-key"),
								Value:     []byte("benchmark-value"),
								Timestamp: 1640995200000,
								Headers:   map[string]string{"test": "header"},
							},
						},
					},
				},
			},
		},
	}

	b.ResetTimer()
	for b.Loop() {
		data, err := request.Serialize()
		if err != nil {
			b.Fatal(err)
		}
		_, err = DeserializeProduceRequest(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
