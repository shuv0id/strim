package protocol

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	t.Run("valid message", func(t *testing.T) {
		msg := &Message{
			Topic:     "test-topic",
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Timestamp: time.Now().UnixMilli(),
		}

		err := msg.Validate()
		assert.NoError(t, err, "expected valid message to pass validation")
	})

	t.Run("empty topic", func(t *testing.T) {
		msg := &Message{
			Topic:     "",
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Timestamp: time.Now().UnixMilli(),
		}

		err := msg.Validate()
		assert.Error(t, err, "expected validation to fail for empty topic")
	})

	t.Run("empty key", func(t *testing.T) {
		msg := &Message{
			Topic:     "test-topic",
			Key:       []byte{},
			Value:     []byte("value1"),
			Timestamp: time.Now().UnixMilli(),
		}

		err := msg.Validate()
		assert.Error(t, err, "expected validation to fail for empty key")
	})

	t.Run("empty value", func(t *testing.T) {
		msg := &Message{
			Topic:     "test-topic",
			Key:       []byte("key1"),
			Value:     []byte{},
			Timestamp: time.Now().UnixMilli(),
		}

		err := msg.Validate()
		assert.Error(t, err, "expected validation to fail for empty value")
	})

	t.Run("zero timestamp", func(t *testing.T) {
		msg := &Message{
			Topic:     "test-topic",
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Timestamp: time.Time{}.UnixMilli(),
		}

		err := msg.Validate()
		assert.Error(t, err, "expected validation to fail for zero timestamp")
	})
}

func TestMessage_RoundTrip(t *testing.T) {
	testCases := []struct {
		name string
		msg  *Message
	}{
		{
			name: "basic message",
			msg: &Message{
				Offset:    12345,
				Timestamp: time.Now().UnixMilli(),
				Topic:     "orders",
				Key:       []byte("order-123"),
				Value:     []byte(`{"order_id": 123, "amount": 99.99}`),
				Headers:   nil,
			},
		},
		{
			name: "message with headers",
			msg: &Message{
				Offset:    67890,
				Timestamp: time.Now().UnixMilli(),
				Topic:     "users",
				Key:       []byte("user-456"),
				Value:     []byte("premiumuser"),
				Headers: map[string]string{
					"version":      "1.2.3",
					"content-type": "application/json",
				},
			},
		},
		{
			name: "empty headers",
			msg: &Message{
				Offset:    0,
				Timestamp: time.Now().UnixMilli(),
				Topic:     "minimal-topic",
				Key:       []byte("k"),
				Value:     []byte("v"),
				Headers:   make(map[string]string),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.msg.Serialize()
			require.NotEmpty(t, data, "serialize should not return empty data")

			deserialized, err := Deserialize(data)
			require.NoError(t, err, "deserialize should not fail")

			assert.Equal(t, tc.msg.Offset, deserialized.Offset, "offset should match")
			assert.Equal(t, tc.msg.Topic, deserialized.Topic, "topic should match")
			assert.Equal(t, tc.msg.Key, deserialized.Key, "key should match")
			assert.Equal(t, tc.msg.Value, deserialized.Value, "value should match")
			assert.Equal(t, tc.msg.Timestamp, deserialized.Timestamp, "timestamp should match")
			assert.Equal(t, len(tc.msg.Headers), len(deserialized.Headers), "header count should match")

			for k, v := range tc.msg.Headers {
				assert.Equal(t, v, deserialized.Headers[k], "header %s should match", k)
			}
		})
	}
}

func TestDeserialize_InvalidData(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty data", []byte{}},
		{"short data", []byte{1, 2, 3}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Deserialize(tc.data)
			assert.Error(t, err, "should return error for invalid data")
		})
	}
}

func TestMessage_Serialize_Concurrent(t *testing.T) {
	msg := &Message{
		Offset:    1000,
		Timestamp: time.Now().UnixMilli(),
		Topic:     "concurrent-test",
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		Headers:   map[string]string{"test": "concurrent"},
	}

	done := make(chan error, 10)

	for i := 0; i < 10; i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					done <- fmt.Errorf("panic during serialize: %v", r)
				} else {
					done <- nil
				}
			}()

			data := msg.Serialize()
			require.NotEmpty(t, data, "serialize should not return empty data")

			_, err := Deserialize(data)
			done <- err
		}()
	}

	for i := 0; i < 10; i++ {
		err := <-done
		assert.NoError(t, err, "concurrent operation should not fail")
	}
}
