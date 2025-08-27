package partition

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Segment struct {
	baseOffset     uint64
	log            *os.File
	index          *os.File
	timeIndex      *os.File
	topic          string // TODO: duplicate data from Partition?
	partition      uint64 // TODO: duplicate data from Partition?
	active         bool
	lastIndexedPos int64
}

func NewSegment(partitionDir string, offset uint64, topic string, partition uint64) (*Segment, error) {
	logPath := filepath.Join(partitionDir, pad(offset)+".log")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening log file for segment %s-%d-%d: %v", topic, partition, offset, err)
	}

	indexPath := filepath.Join(partitionDir, pad(offset)+".index")
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening index file for segment %s-%d-%d: %v", topic, partition, offset, err)
	}

	timeindexPath := filepath.Join(partitionDir, pad(offset)+".timeindex")
	timeindexFile, err := os.OpenFile(timeindexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening timeindex file for segment %s-%d-%d: %v", topic, partition, offset, err)
	}
	return &Segment{
		baseOffset: offset,
		log:        file,
		index:      indexFile,
		timeIndex:  timeindexFile,
		topic:      topic,
		partition:  partition,
		active:     true,
	}, nil
}

func (s *Segment) Size() (int64, error) {
	fileInfo, err := os.Stat(s.log.Name())
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}

func (s *Segment) IndexSize() (int64, error) {
	fileInfo, err := os.Stat(s.index.Name())
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}

func (s *Segment) TimeIndexSize() (int64, error) {
	fileInfo, err := os.Stat(s.timeIndex.Name())
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}

func (s *Segment) writeMsg(msgBytes []byte) error {
	if !s.active {
		return fmt.Errorf("segment not active: %s-%d-%d", s.topic, s.partition, s.baseOffset)
	}

	_, err := s.log.Write(msgBytes)
	if err != nil {
		return fmt.Errorf("error writing to segment: %s-%d-%d: %v", s.topic, s.partition, s.baseOffset, err)
	}
	return nil
}

func (s *Segment) appendIndexEntry(offset uint64, pos uint32) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], uint32(offset-s.baseOffset))
	binary.BigEndian.PutUint32(buf[4:8], pos)

	_, err := s.index.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) appendTimeIndexEntry(t time.Time, relativeOffset uint32) error {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], uint64(t.UnixMilli()))
	binary.BigEndian.PutUint32(buf[8:12], relativeOffset)

	_, err := s.timeIndex.Write(buf)
	if err != nil {
		return err
	}
	return nil
}
func (s *Segment) Close() error {
	if err := s.log.Close(); err != nil {
		return fmt.Errorf("error closing log file for segment: %s-%d-%d: %v", s.topic, s.partition, s.baseOffset, err)
	}
	if err := s.index.Close(); err != nil {
		return fmt.Errorf("error closing index file for segment: %s-%d-%d: %v", s.topic, s.partition, s.baseOffset, err)
	}
	if err := s.timeIndex.Close(); err != nil {
		return fmt.Errorf("error closing timeindex file for segment: %s-%d-%d: %v", s.topic, s.partition, s.baseOffset, err)
	}

	s.active = false
	return nil
}

func pad(baseOffset uint64) string {
	s := strconv.FormatUint(baseOffset, 10)
	if len(s) >= 20 {
		return s
	}
	return strings.Repeat("0", 20-len(s)) + s
}
