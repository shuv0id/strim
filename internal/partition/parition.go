package partition

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/shuv0id/strim/internal/config"
)

type BatchError struct {
	batchIndex int
	errorMsg   string
}

type BatchErrors struct {
	BaseOffset int
	Errors     []*BatchError
}

func (batchError *BatchError) Error() string {
	return fmt.Sprintf("batch index:%d: %s", batchError.batchIndex, batchError.errorMsg)
}

func (be *BatchError) BatchIndex() int {
	return be.batchIndex
}

type Partition interface {
	Append(msgs []*Message) *BatchErrors
	Read(offset uint64, maxBytes uint64) ([]*Message, error)
	GetPartitionIndex() uint32
	GetTopic() string
	GetLEO() uint64
	GetHWM() uint64
}

type LogPartition struct {
	Index         uint32 // ID with 0 will considered as leader, rest will be followers partition
	Topic         string
	LEO           uint64 // log-end-offset: The offset of the next message to be written in a partition.
	HWM           uint64 // High Watermark: the highest offset that is safely replicated to all replicas.(leader only; followers will default to 0)
	ActiveSegment *Segment
	Segments      []*Segment // map for storing baseOffset of each segment to its the Segment itself of quick lookups
	Config        *config.LogConfig
	mu            sync.RWMutex
}

func NewPartition(index uint32, topic string, cfg *config.LogConfig) (Partition, error) {
	dirName := fmt.Sprintf("%s-%d", topic, index)
	if err := os.Mkdir(dirName, 0755); err != nil {
		return nil, err
	}

	s, err := NewSegment(dirName, 0, topic, index)
	if err != nil {
		return nil, err
	}
	var segments []*Segment
	segments = append(segments, s)

	return &LogPartition{
		Index:         index,
		Topic:         topic,
		LEO:           0,
		HWM:           0,
		ActiveSegment: s,
		Segments:      segments,
		Config:        cfg,
	}, nil
}

func (p *LogPartition) Append(msgs []*Message) *BatchErrors {
	batchErrs := &BatchErrors{
		Errors: make([]*BatchError, 0, len(msgs)),
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(msgs) == 0 {
		batchError := &BatchError{
			batchIndex: 0,
			errorMsg:   "empty batch messages",
		}
		batchErrs.Errors = append(batchErrs.Errors, batchError)
		return batchErrs
	}

	activeSeg := p.ActiveSegment

	for i, m := range msgs {
		if err := m.Validate(); err != nil {
			batchErr := BatchError{
				batchIndex: i,
				errorMsg:   fmt.Sprintf("invalid message: %v", err),
			}
			batchErrs.Errors = append(batchErrs.Errors, &batchErr)
			continue
		}
		if m.Offset != p.LEO {
			batchErr := BatchError{
				batchIndex: i,
				errorMsg:   fmt.Sprintf("incorrect message offset: expected: %d got: %d", p.LEO, m.Offset),
			}
			batchErrs.Errors = append(batchErrs.Errors, &batchErr)
			continue
		}

		msgData := m.Serialize()
		msgSize := int64(len(msgData))

		if roll, _, err := p.shouldRollSegment(msgSize); err != nil {
			// NOTE: will be adding log here
			// return err
		} else if roll {
			if err := p.rollSegment(m.Offset); err != nil {
				// NOTE: will be adding log here
				// return err
			}
			activeSeg = p.ActiveSegment
		}

		startPos, err := activeSeg.Size()
		if err != nil {
			// NOTE: will be adding log here
			// return err
		}

		payload := make([]byte, 4+msgSize)
		binary.BigEndian.PutUint32(payload[:4], uint32(msgSize))
		copy(payload[4:], msgData)

		if err = activeSeg.write(payload); err != nil {
			// NOTE: will be adding log here
			batchErr := BatchError{batchIndex: i, errorMsg: "internal server error"}
			batchErrs.Errors = append(batchErrs.Errors, &batchErr)
			continue
		}
		batchErrs.BaseOffset = int(m.Offset)
		p.LEO++

		endPos := startPos + int64(len(payload))

		bytesSinceLastIndex := endPos - activeSeg.lastIndexedPos
		if bytesSinceLastIndex >= p.Config.IndexIntervalBytes {
			relativeOffset := m.Offset - activeSeg.baseOffset

			err := activeSeg.appendIndexEntry(relativeOffset, uint32(startPos))
			if err != nil {
				// NOTE: will be adding log here
				// return fmt.Errorf("error making index entry for message: %s-%d: %v", m.Topic, m.Offset, err)
			}
			err = activeSeg.appendTimeIndexEntry(m.Timestamp, uint32(relativeOffset))
			if err != nil {
				// NOTE: will be adding log here
				// return fmt.Errorf("error making timeindex entry for message: %s-%d: %v", m.Topic, m.Offset, err)
			}

			activeSeg.lastIndexedPos = endPos
		}
	}
	return batchErrs
}

func (p *LogPartition) Read(offset uint64, maxBytes uint64) ([]*Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	seg, err := p.findSegmentForOffset(offset)
	if err != nil {
		return nil, err
	}

	pos, err := seg.findPositionInIndex(offset)
	if err != nil {
		return nil, fmt.Errorf("error finding position of message from index: %v", err)
	}

	var messages []*Message
	bytesRead := 0
	for bytesRead < int(maxBytes) {
		lenBytes, err := seg.read(int64(pos), 4)
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
		msgBytes, err := seg.read(int64(pos)+4, int(msgLen))
		if err != nil {
			return nil, fmt.Errorf("error reading log for segment: %v", err)
		}

		msg, err := Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
		bytesRead += 4 + int(msgLen)
		pos += 4 + uint32(msgLen)
	}
	return messages, nil
}

func (p *LogPartition) GetPartitionIndex() uint32 {
	return p.Index
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

func (p *LogPartition) findSegmentForOffset(offset uint64) (*Segment, error) {
	start := 0
	end := len(p.Segments) - 1

	if offset < p.Segments[start].baseOffset || offset > p.Segments[end].baseOffset {
		return nil, fmt.Errorf("segment not found for the offset %d", offset)
	}

	for start <= end {
		mid := start + (end-start)/2
		seg := p.Segments[mid]

		if offset >= seg.baseOffset && (mid == len(p.Segments)-1 || offset < p.Segments[mid+1].baseOffset) {
			return seg, nil
		}
		if offset < seg.baseOffset {
			end = mid - 1
		} else {
			start = mid + 1
		}
	}

	return nil, fmt.Errorf("segment not found for the offset %d", offset)
}

func (p *LogPartition) rollSegment(baseOffset uint64) error {
	partitionDir := fmt.Sprintf("%s-%d", p.Topic, p.Index)
	newSeg, err := NewSegment(partitionDir, baseOffset, p.Topic, p.Index)
	if err != nil {
		return err
	}

	if err = p.makeSegmentInactive(p.ActiveSegment); err != nil {
		return fmt.Errorf("error making current segment inactive when rolling: %v", err)
	}

	p.ActiveSegment = newSeg
	p.Segments = append(p.Segments, newSeg)
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

func (p *LogPartition) makeSegmentInactive(seg *Segment) error {
	if err := seg.log.Sync(); err != nil {
		return fmt.Errorf("failed to sync log: %w", err)
	}
	if err := seg.index.Sync(); err != nil {
		return fmt.Errorf("failed to sync index: %w", err)
	}
	if err := seg.timeIndex.Sync(); err != nil {
		return fmt.Errorf("failed to sync time index: %w", err)
	}

	seg.markInactive()

	return nil
}
