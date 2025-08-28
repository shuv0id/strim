package partition

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/shuv0id/strim/internal/config"
	"github.com/shuv0id/strim/pkg/protocol"
)

type Partition interface {
	Append(msgs []*protocol.Message) error
	Read(offset uint64, maxBytes uint64) ([]*protocol.Message, error)
	GetTopic() string
	GetLEO() uint64
	GetHWM() uint64
}

type LogPartition struct {
	ID            uint64 // ID with 0 will considered as leader, rest will be followers partition
	Topic         string
	LEO           uint64 // log-end-offset: The offset of the next message to be written in a partition.
	HWM           uint64 // High Watermark: the highest offset that is safely replicated to all replicas.(leader only; followers will default to 0)
	ActiveSegment *Segment
	Config        *config.LogConfig
	mu            sync.RWMutex
}

func NewPartition(id uint64, topic string) (Partition, error) {
	dir := filepath.Join(topic, "-", strconv.FormatUint(id, 10))
	if err := os.Mkdir(dir, 0755); err != nil {
		return nil, err
	}

	s, err := NewSegment(dir, 0, topic, id)
	if err != nil {
		return nil, err
	}

	return &LogPartition{
		ID:            id,
		Topic:         topic,
		LEO:           0,
		HWM:           0,
		ActiveSegment: s,
	}, nil
}

func (p *LogPartition) Append(msgs []*protocol.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	seg := p.ActiveSegment

	for _, m := range msgs {
		if err := m.Validate(); err != nil {
			return err
		}
		if m.Offset != p.LEO {
			return fmt.Errorf("incorrect message offset: expected: %d got: %d", p.LEO, m.Offset)
		}

		msgData := m.Serialize()
		msgSize := int64(len(msgData))

		if roll, _, err := p.shouldRollSegment(msgSize); err != nil {
			return err
		} else if roll {
			if err := p.rollSegment(m.Offset); err != nil {
				return err
			}
			seg = p.ActiveSegment
		}

		startPos, err := seg.Size()
		if err != nil {
			return err
		}

		if err = seg.write(msgData); err != nil {
			return err
		}
		p.LEO++

		endPos := startPos + msgSize

		bytesSinceLastIndex := endPos - seg.lastIndexedPos
		if bytesSinceLastIndex >= p.Config.IndexIntervalBytes {
			relativeOffset := m.Offset - seg.baseOffset

			err := seg.appendIndexEntry(relativeOffset, uint32(startPos))
			if err != nil {
				return fmt.Errorf("error making index entry for message: %s-%d: %v", m.Topic, m.Offset, err)
			}
			err = seg.appendTimeIndexEntry(m.Timestamp, uint32(relativeOffset))
			if err != nil {
				return fmt.Errorf("error making timeindex entry for message: %s-%d: %v", m.Topic, m.Offset, err)
			}

			seg.lastIndexedPos = endPos
		}
	}
	return nil
}

func (p *LogPartition) Read(offset uint64, maxBytes uint64) ([]*protocol.Message, error) {
	pos, err := p.ActiveSegment.findPositionInIndex(offset)
	if err != nil {
		return nil, fmt.Errorf("error finding position of message from index: %v", err)
	}

	var messages []*protocol.Message
	bytesRead := 0
	for bytesRead < int(maxBytes) {
		lenBytes, err := p.ActiveSegment.read(int64(pos), 4)
		if err != nil {
			return nil, fmt.Errorf("error reading log for segment: %v", err)
		}
		if len(lenBytes) < 4 {
			break
		}

		msgLen := binary.BigEndian.Uint32(lenBytes)

		if bytesRead+int(msgLen) > int(maxBytes) {
			break
		}
		msgBytes, err := p.ActiveSegment.read(int64(pos)+4, int(msgLen))
		if err != nil {
			return nil, fmt.Errorf("error reading log for segment: %v", err)
		}

		msg, err := protocol.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
		bytesRead += 4 + int(msgLen)
		pos += 4 + uint32(msgLen)
	}
	return messages, nil
}

func (p *LogPartition) GetTopic() string {
	return p.Topic
}

func (p *LogPartition) GetLEO() uint64 {
	return p.LEO
}

func (p *LogPartition) GetHWM() uint64 {
	return p.HWM
}

func (p *LogPartition) rollSegment(baseOffset uint64) error {
	partitionDir := filepath.Join(p.Topic, "-", strconv.FormatUint(p.ID, 10))
	newSeg, err := NewSegment(partitionDir, baseOffset, p.Topic, p.ID)
	if err != nil {
		return err
	}
	p.ActiveSegment = newSeg
	return nil
}

func (p *LogPartition) shouldRollSegment(msgSize int64) (bool, string, error) {
	segSize, err := p.ActiveSegment.Size()
	if err != nil {
		return false, "", fmt.Errorf("failed to get segment size: %v", err)
	}
	if segSize+msgSize > p.Config.SegmentBytes {
		return true, "segment_size_limit", nil
	}

	indexSize, err := p.ActiveSegment.IndexSize()
	if err != nil {
		return false, "", fmt.Errorf("failed to get index size: %v", err)
	}
	if indexSize > p.Config.IndexMaxBytes {
		return true, "index_size_limit", nil
	}

	timeIndexSize, err := p.ActiveSegment.TimeIndexSize()
	if err != nil {
		return false, "", fmt.Errorf("failed to get timeIndex size: %v", err)
	}
	if timeIndexSize > p.Config.IndexMaxBytes {
		return true, "timeIndex_size_limit", nil
	}

	return false, "", nil
}
