package partition

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDir(t *testing.T) string {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "segment_test_*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})
	return tmpDir
}

func createTestSegment(t *testing.T, dir string, offset uint64, topic string, partition uint64) *Segment {
	s, err := NewSegment(dir, offset, topic, partition)
	require.NoError(t, err)
	t.Cleanup(func() {
		s.markInactive()
	})
	return s
}

func TestNewSegment(t *testing.T) {
	testDir := setupTestDir(t)
	testCases := []struct {
		name      string
		offset    uint64
		topic     string
		partition uint64
		wantErr   bool
	}{
		{
			name:      "valid segment initialization",
			offset:    0,
			topic:     "test-topic",
			partition: 0,
		},
	}
	t.Run("valid segment creation", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				s, err := NewSegment(testDir, tc.offset, tc.topic, tc.partition)
				if (err != nil) != tc.wantErr {
					t.Errorf("%q expected error=%v, got error=%v", tc.name, tc.wantErr, err)
					return
				}
				if !tc.wantErr {
					require.NotNil(t, s)
					defer s.markInactive()

					assert.Equal(t, tc.offset, s.baseOffset)
					assert.Equal(t, tc.topic, s.topic)
					assert.Equal(t, tc.partition, s.partition)
					assert.True(t, s.active)

					expectedFiles := []string{
						filepath.Join(testDir, pad(tc.offset)+".log"),
						filepath.Join(testDir, pad(tc.offset)+".index"),
						filepath.Join(testDir, pad(tc.offset)+".timeindex"),
					}

					for _, filepath := range expectedFiles {
						assert.FileExists(t, filepath)
					}
				}
			})
		}

	})
}

func TestNewSegment_InvalidDirectory(t *testing.T) {
	invalidDir := "/non-existent/direcory"
	_, err := NewSegment(invalidDir, 0, "user", 0)
	assert.Error(t, err)
}

func TestSegment_WriteMsg(t *testing.T) {
	dir := setupTestDir(t)
	s := createTestSegment(t, dir, 0, "user", 0)

	testcases := []struct {
		name    string
		message []byte
	}{
		{
			name:    "simple message",
			message: []byte("hello world"),
		},
		{
			name:    "empty message",
			message: []byte(""),
		},
		{
			name:    "large message",
			message: make([]byte, 10000),
		},
	}

	for _, tc := range testcases {
		initialSize, err := s.Size()
		require.NoError(t, err)

		err = s.write(tc.message)
		require.NoError(t, err)

		newSize, err := s.Size()
		require.NoError(t, err)

		expectedSize := initialSize + int64(len(tc.message))
		assert.Equal(t, expectedSize, newSize)
	}
}

func TestSegment_WriteMsg_Inactive_Segment(t *testing.T) {
	testDir := setupTestDir(t)
	s := createTestSegment(t, testDir, 0, "user", 0)
	s.markInactive()

	err := s.write([]byte("user created"))
	assert.Error(t, err)
}

func TestSegment_AppendIndexEntry(t *testing.T) {
	testDir := setupTestDir(t)
	s := createTestSegment(t, testDir, 0, "user", 0)

	testcases := []struct {
		name   string
		offset uint64
		pos    uint32
	}{
		{
			name:   "append valid index entry",
			offset: 6969,
			pos:    420,
		},
	}

	for _, tc := range testcases {
		initialSize, err := s.IndexSize()
		require.NoError(t, err)

		err = s.appendIndexEntry(tc.offset, tc.pos)
		assert.NoError(t, err)

		newSize, err := s.IndexSize()
		require.NoError(t, err)

		assert.Equal(t, initialSize+8, newSize)

		_, err = s.index.Seek(-8, 2)
		require.NoError(t, err)
		buf := make([]byte, 8)
		_, err = s.index.Read(buf)
		require.NoError(t, err)

		expectedRelativeOffset := uint32(tc.offset - s.baseOffset)
		actualRelativeOffset := binary.BigEndian.Uint32(buf[0:4])

		assert.Equal(t, expectedRelativeOffset, actualRelativeOffset)

		actualPos := binary.BigEndian.Uint32(buf[4:8])
		assert.Equal(t, tc.pos, actualPos)

	}

}

func TestSegment_AppendTimeIndexEntry(t *testing.T) {
	testDir := setupTestDir(t)
	s := createTestSegment(t, testDir, 0, "user", 0)

	testcases := []struct {
		name           string
		time           time.Time
		relativeOffset uint32
	}{
		{
			name:           "append valid timeindex entry",
			time:           time.Now(),
			relativeOffset: 69,
		},
	}

	for _, tc := range testcases {
		initialSize, err := s.TimeIndexSize()
		require.NoError(t, err)

		err = s.appendTimeIndexEntry(tc.time, tc.relativeOffset)
		assert.NoError(t, err)

		newSize, err := s.TimeIndexSize()
		require.NoError(t, err)

		assert.Equal(t, initialSize+12, newSize)

		_, err = s.timeIndex.Seek(-12, 2)
		require.NoError(t, err)
		buf := make([]byte, 12)
		_, err = s.timeIndex.Read(buf)
		require.NoError(t, err)

		expectedTimestamps := uint64(tc.time.UnixMilli())
		actualTimestamps := binary.BigEndian.Uint64(buf[0:8])
		assert.Equal(t, expectedTimestamps, actualTimestamps)

		actualRelativeOffset := binary.BigEndian.Uint32(buf[8:12])
		assert.Equal(t, tc.relativeOffset, actualRelativeOffset)

	}

}

func TestSegment_markInactive(t *testing.T) {
	testDir := setupTestDir(t)
	segment := createTestSegment(t, testDir, 0, "user", 0)

	assert.True(t, segment.active)

	err := segment.write([]byte("test"))
	require.NoError(t, err)
	err = segment.appendIndexEntry(5, 0)
	require.NoError(t, err)
	err = segment.appendTimeIndexEntry(time.Now(), 0)
	require.NoError(t, err)

	segment.markInactive()

	err = segment.write([]byte("should fail"))
	assert.Error(t, err, "Expected error when writing to inactive segment")
}

func TestPad(t *testing.T) {
	testcases := []struct {
		input    uint64
		expected string
	}{
		{0, "00000000000000000000"},
		{123, "00000000000000000123"},
		{9999999999999999999, "09999999999999999999"},
		{10000000000000000000, "10000000000000000000"},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.expected, pad(tc.input))
	}
}

func BenchmarkSegment_WriteMsg(b *testing.B) {
	testDir, _ := os.MkdirTemp("", "segment_bench_*")
	defer os.RemoveAll(testDir)

	s, _ := NewSegment(testDir, 0, "topic-benchmark", 0)
	defer s.markInactive()

	message := []byte("benchmark message for test write performance")

	b.ResetTimer()
	for b.Loop() {
		s.write(message)
	}
}
