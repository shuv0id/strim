package protocol

import (
	"fmt"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	t.Run("valid message", func(t *testing.T) {
		msg := &Message{
			Topic:     "test-topic",
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Timestamp: time.Now(),
		}

		err := msg.Validate()
		if err != nil {
			t.Errorf("expected valid message to pass validation, got: %v", err)
		}
	})

	t.Run("empty topic", func(t *testing.T) {
		msg := &Message{
			Topic:     "",
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Timestamp: time.Now(),
		}

		err := msg.Validate()
		if err == nil {
			t.Error("expected validation to fail for empty topic")
		}
	})

	t.Run("empty key", func(t *testing.T) {
		msg := &Message{
			Topic:     "test-topic",
			Key:       []byte{},
			Value:     []byte("value1"),
			Timestamp: time.Now(),
		}

		err := msg.Validate()
		if err == nil {
			t.Error("expected validation to fail for empty key")
		}
	})

	t.Run("empty value", func(t *testing.T) {
		msg := &Message{
			Topic:     "test-topic",
			Key:       []byte("key1"),
			Value:     []byte{},
			Timestamp: time.Now(),
		}

		err := msg.Validate()
		if err == nil {
			t.Error("expected validation to fail for empty value")
		}
	})

	t.Run("zero timestamp", func(t *testing.T) {
		msg := &Message{
			Topic:     "test-topic",
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Timestamp: time.Time{},
		}

		err := msg.Validate()
		if err == nil {
			t.Error("expected validation to fail for zero timestamp")
		}
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
				Timestamp: time.Now(),
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
				Timestamp: time.Now(),
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
				Timestamp: time.Now(),
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
			if len(data) == 0 {
				t.Fatal("serialize returned empty data")
			}

			deserialized, err := Deserialize(data)
			if err != nil {
				t.Fatalf("deserialize failed: %v", err)
			}

			if deserialized.Offset != tc.msg.Offset {
				t.Errorf("offset mismatch: expected %d, got %d",
					tc.msg.Offset, deserialized.Offset)
			}

			if deserialized.Topic != tc.msg.Topic {
				t.Errorf("topic mismatch: expected %s, got %s",
					tc.msg.Topic, deserialized.Topic)
			}

			if string(deserialized.Key) != string(tc.msg.Key) {
				t.Errorf("key mismatch: expected %v, got %v",
					tc.msg.Key, deserialized.Key)
			}

			if string(deserialized.Value) != string(tc.msg.Value) {
				t.Errorf("value mismatch: expected %v, got %v",
					tc.msg.Value, deserialized.Value)
			}

			if len(deserialized.Headers) != len(tc.msg.Headers) {
				t.Errorf("headers count mismatch: expected %d, got %d",
					len(tc.msg.Headers), len(deserialized.Headers))
			}

			for k, v := range tc.msg.Headers {
				if deserialized.Headers[k] != v {
					t.Errorf("header %s mismatch: expected %s, got %s",
						k, v, deserialized.Headers[k])
				}
			}

			timeDiff := deserialized.Timestamp.Sub(tc.msg.Timestamp)
			if timeDiff > time.Millisecond || timeDiff < -time.Millisecond {
				t.Errorf("timestamp mismatch: expected %v, got %v (diff: %v)",
					tc.msg.Timestamp, deserialized.Timestamp, timeDiff)
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
			if err == nil {
				t.Error("expected error for invalid data")
			}
		})
	}
}

func TestMessage_Serialize_Concurrent(t *testing.T) {
	msg := &Message{
		Offset:    1000,
		Timestamp: time.Now(),
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
			if len(data) == 0 {
				done <- fmt.Errorf("serialize returned empty data")
				return
			}

			_, err := Deserialize(data)
			done <- err
		}()
	}

	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Errorf("concurrent operation failed: %v", err)
		}
	}
}
