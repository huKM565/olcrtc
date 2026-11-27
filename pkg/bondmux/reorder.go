package bondmux

import (
	"sync"
	"sync/atomic"
)

type ReorderBuffer struct {
	mu           sync.Mutex
	nextOffset   uint64
	streamWindow uint64
	chunks       map[uint64][]byte
	buffered     int
	finSet       bool
	finOffset    uint64
	sessionBytes *atomic.Int64
	sessionCap   int64
	closed       bool
}

func NewReorderBuffer(streamWindow uint32, sessionBytes *atomic.Int64, sessionCap int64) *ReorderBuffer {
	return &ReorderBuffer{
		streamWindow: uint64(streamWindow),
		chunks:       make(map[uint64][]byte),
		sessionBytes: sessionBytes,
		sessionCap:   sessionCap,
	}
}

func (rb *ReorderBuffer) Push(offset uint64, data []byte) ([][]byte, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.closed {
		return nil, false
	}
	if len(data) == 0 {
		return nil, true
	}

	end := offset + uint64(len(data))
	if end <= rb.nextOffset {
		return nil, true
	}
	if offset < rb.nextOffset {
		trim := rb.nextOffset - offset
		data = data[trim:]
		offset = rb.nextOffset
	}
	if offset+uint64(len(data)) > rb.nextOffset+rb.streamWindow {
		return nil, false
	}

	if offset > rb.nextOffset {
		if existing, ok := rb.chunks[offset]; ok {
			if len(existing) >= len(data) {
				return nil, true
			}
			delta := int64(len(data) - len(existing))
			if rb.sessionBytes != nil && rb.sessionCap > 0 && rb.sessionBytes.Load()+delta > rb.sessionCap {
				return nil, false
			}
			cp := append([]byte(nil), data...)
			rb.chunks[offset] = cp
			rb.buffered += int(delta)
			if rb.sessionBytes != nil {
				rb.sessionBytes.Add(delta)
			}
			return nil, true
		}
		delta := int64(len(data))
		if rb.sessionBytes != nil && rb.sessionCap > 0 && rb.sessionBytes.Load()+delta > rb.sessionCap {
			return nil, false
		}
		cp := append([]byte(nil), data...)
		rb.chunks[offset] = cp
		rb.buffered += len(cp)
		if rb.sessionBytes != nil {
			rb.sessionBytes.Add(delta)
		}
		return nil, true
	}

	out := [][]byte{append([]byte(nil), data...)}
	rb.nextOffset += uint64(len(data))

	for len(rb.chunks) > 0 {
		progressed := false
		for off, chunk := range rb.chunks {
			cEnd := off + uint64(len(chunk))
			if cEnd <= rb.nextOffset {
				delete(rb.chunks, off)
				rb.buffered -= len(chunk)
				if rb.sessionBytes != nil {
					rb.sessionBytes.Add(-int64(len(chunk)))
				}
				progressed = true
				continue
			}
			if off <= rb.nextOffset {
				delete(rb.chunks, off)
				rb.buffered -= len(chunk)
				if rb.sessionBytes != nil {
					rb.sessionBytes.Add(-int64(len(chunk)))
				}
				trim := rb.nextOffset - off
				piece := chunk[trim:]
				out = append(out, piece)
				rb.nextOffset += uint64(len(piece))
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	return out, true
}

func (rb *ReorderBuffer) MarkFin(finalOffset uint64) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.finSet = true
	rb.finOffset = finalOffset
	return rb.nextOffset >= rb.finOffset
}

func (rb *ReorderBuffer) IsEOF() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.finSet && rb.nextOffset >= rb.finOffset
}

func (rb *ReorderBuffer) NextOffset() uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.nextOffset
}

func (rb *ReorderBuffer) BufferedBytes() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.buffered
}

func (rb *ReorderBuffer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.closed {
		return
	}
	rb.closed = true
	if rb.buffered > 0 && rb.sessionBytes != nil {
		rb.sessionBytes.Add(-int64(rb.buffered))
	}
	rb.buffered = 0
	rb.chunks = nil
}
