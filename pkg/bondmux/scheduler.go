package bondmux

import (
	"math/rand"
	"slices"
	"sync"
	"time"
)

const (
	initialPipeCapBytes int64 = 96 << 10
	maxPipeCapBytes     int64 = 512 << 10
	minPipeCapBytes     int64 = 48 << 10
	initialEstBW              = 512.0 * 1024
	minEstBW                  = 64.0 * 1024
	initialRTT                = 80 * time.Millisecond
	maxPipeRTT                = 2 * time.Second
	minRescueTimeout          = 450 * time.Millisecond
	maxRescueTimeout          = 3 * time.Second
	maxChunkRetries           = 8
	scoreSpreadFactor         = 1.5
)

type Pipe struct {
	ID            uint16
	Writer        *PipeWriter
	ewmaRTT       time.Duration
	estBW         float64
	inFlightBytes int64
	capBytes      int64
	hasSamples    bool
	ackedBytes    int64
	bwWindowStart time.Time
	stalledUntil  time.Time
	closed        bool
}

func NewPipe(id uint16) *Pipe {
	return &Pipe{
		ID:            id,
		ewmaRTT:       initialRTT,
		estBW:         initialEstBW,
		capBytes:      initialPipeCapBytes,
		bwWindowStart: time.Now(),
	}
}

func (p *Pipe) SetStatsForTest(rtt time.Duration, bw float64, inFlight int64) {
	p.ewmaRTT = rtt
	p.estBW = bw
	p.inFlightBytes = inFlight
	p.hasSamples = true
}

func (p *Pipe) score(chunkLen int) float64 {
	rttSec := p.ewmaRTT.Seconds()
	if rttSec < 0.001 {
		rttSec = 0.001
	}
	bw := p.estBW
	if bw < minEstBW {
		bw = minEstBW
	}
	return rttSec + float64(p.inFlightBytes+int64(chunkLen))/bw
}

type InFlightFrame struct {
	ChunkID     uint32
	Frame       Frame
	PipeID      uint16
	SentAt      time.Time
	FirstSentAt time.Time
	Retries     int
	Size        int
}

type Scheduler struct {
	mu       sync.Mutex
	pipes    map[uint16]*Pipe
	inFlight map[uint32]*InFlightFrame
	rng      *rand.Rand
}

func NewScheduler(rng *rand.Rand) *Scheduler {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Scheduler{
		pipes:    make(map[uint16]*Pipe),
		inFlight: make(map[uint32]*InFlightFrame),
		rng:      rng,
	}
}

func (s *Scheduler) RegisterPipe(p *Pipe) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pipes[p.ID] = p
}

func (s *Scheduler) RemovePipe(pipeID uint16) []InFlightFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.pipes[pipeID]; ok {
		p.closed = true
		delete(s.pipes, pipeID)
	}
	var orphaned []InFlightFrame
	for id, item := range s.inFlight {
		if item.PipeID == pipeID {
			item.Retries++
			orphaned = append(orphaned, *item)
			delete(s.inFlight, id)
		}
	}
	return orphaned
}

func (s *Scheduler) PipeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pipes)
}

func (s *Scheduler) PipesSnapshot() []*Pipe {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Pipe, 0, len(s.pipes))
	for _, p := range s.pipes {
		if !p.closed {
			out = append(out, p)
		}
	}
	return out
}

func (s *Scheduler) PickPipe(chunkLen int) (*Pipe, bool) {
	return s.PickPipeExcluding(chunkLen, 0, false)
}

func (s *Scheduler) PickPipeExcluding(chunkLen int, excludeID uint16, exclude bool) (*Pipe, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	type candidate struct {
		pipe  *Pipe
		score float64
	}
	var avail []candidate
	anySampled := false
	for _, p := range s.pipes {
		if p.closed || (exclude && p.ID == excludeID) {
			continue
		}
		if p.inFlightBytes >= p.capBytes {
			continue
		}
		if p.hasSamples {
			anySampled = true
		}
		avail = append(avail, candidate{pipe: p, score: p.score(chunkLen)})
	}
	if len(avail) == 0 && exclude {
		for _, p := range s.pipes {
			if !p.closed && p.inFlightBytes < p.capBytes {
				avail = append(avail, candidate{pipe: p, score: p.score(chunkLen)})
			}
		}
	}
	if len(avail) == 0 {
		return nil, false
	}
	if !anySampled {
		return avail[s.rng.Intn(len(avail))].pipe, true
	}

	best := avail[0].score
	for _, c := range avail[1:] {
		if c.score < best {
			best = c.score
		}
	}
	limit := best * scoreSpreadFactor
	var filtered []candidate
	totalWeight := 0.0
	for _, c := range avail {
		if c.score <= limit {
			w := 1.0 / c.score
			filtered = append(filtered, candidate{pipe: c.pipe, score: w})
			totalWeight += w
		}
	}
	r := s.rng.Float64() * totalWeight
	acc := 0.0
	for _, c := range filtered {
		acc += c.score
		if r <= acc {
			return c.pipe, true
		}
	}
	return filtered[len(filtered)-1].pipe, true
}

func (s *Scheduler) FastestPipe() (*Pipe, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var bestHealthy *Pipe
	var bestAny *Pipe
	for _, p := range s.pipes {
		if p.closed {
			continue
		}
		if bestAny == nil || p.ewmaRTT < bestAny.ewmaRTT {
			bestAny = p
		}
		if !now.Before(p.stalledUntil) {
			if bestHealthy == nil || p.ewmaRTT < bestHealthy.ewmaRTT {
				bestHealthy = p
			}
		}
	}
	if bestHealthy != nil {
		return bestHealthy, true
	}
	return bestAny, bestAny != nil
}

func (s *Scheduler) PickUDPPipe(flowID uint32, prevPipeID uint16) (*Pipe, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if prevPipeID != 0 {
		if p, ok := s.pipes[prevPipeID]; ok && !p.closed && !now.Before(p.stalledUntil) {
			return p, true
		}
	}
	var healthy []*Pipe
	var all []*Pipe
	for _, p := range s.pipes {
		if p.closed {
			continue
		}
		all = append(all, p)
		if !now.Before(p.stalledUntil) {
			healthy = append(healthy, p)
		}
	}
	pool := healthy
	if len(pool) == 0 {
		pool = all
	}
	if len(pool) == 0 {
		return nil, false
	}
	slices.SortFunc(pool, func(a, b *Pipe) int {
		if a.ewmaRTT < b.ewmaRTT {
			return -1
		}
		if a.ewmaRTT > b.ewmaRTT {
			return 1
		}
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	bestRTT := pool[0].ewmaRTT
	limit := max(time.Duration(float64(bestRTT)*1.3), bestRTT+20*time.Millisecond)
	candidates := pool[:1]
	for _, p := range pool[1:] {
		if p.ewmaRTT <= limit {
			candidates = append(candidates, p)
		}
	}
	return candidates[int(flowID)%len(candidates)], true
}

func (s *Scheduler) UpdatePipeRTT(pipeID uint16, rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pipes[pipeID]
	if !ok {
		return
	}
	if !p.hasSamples {
		p.ewmaRTT = rtt
		p.hasSamples = true
	} else {
		p.ewmaRTT = (p.ewmaRTT*7 + rtt) / 8
	}
	s.adaptCapLocked(p)
}

func (s *Scheduler) TrackInFlight(chunkID uint32, f Frame, pipeID uint16, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	size := HeaderSize + len(f.Payload)
	if old, ok := s.inFlight[chunkID]; ok {
		if p, pok := s.pipes[old.PipeID]; pok {
			p.inFlightBytes = max(0, p.inFlightBytes-int64(old.Size))
		}
	}
	s.inFlight[chunkID] = &InFlightFrame{
		ChunkID:     chunkID,
		Frame:       f,
		PipeID:      pipeID,
		SentAt:      now,
		FirstSentAt: now,
		Size:        size,
	}
	if p, ok := s.pipes[pipeID]; ok {
		p.inFlightBytes += int64(size)
	}
}

func (s *Scheduler) TrackRetransmit(item InFlightFrame, newPipeID uint16, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item.PipeID = newPipeID
	item.SentAt = now
	s.inFlight[item.ChunkID] = &item
	if p, ok := s.pipes[newPipeID]; ok {
		p.inFlightBytes += int64(item.Size)
	}
}

func (s *Scheduler) OnAck(chunkID uint32, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.inFlight[chunkID]
	if !ok {
		return false
	}
	delete(s.inFlight, chunkID)
	p, pok := s.pipes[item.PipeID]
	if !pok {
		return true
	}
	p.inFlightBytes = max(0, p.inFlightBytes-int64(item.Size))
	p.stalledUntil = time.Time{}
	rtt := now.Sub(item.SentAt)
	if rtt > 0 && item.Retries == 0 {
		if !p.hasSamples {
			p.ewmaRTT = min(rtt, maxPipeRTT)
			p.hasSamples = true
		} else {
			p.ewmaRTT = min((p.ewmaRTT*7+rtt)/8, maxPipeRTT)
		}
	}
	p.ackedBytes += int64(item.Size)
	elapsed := now.Sub(p.bwWindowStart)
	if elapsed >= 500*time.Millisecond {
		sampleBW := float64(p.ackedBytes) / elapsed.Seconds()
		if sampleBW < minEstBW {
			sampleBW = minEstBW
		}
		p.estBW = p.estBW*0.7 + sampleBW*0.3
		p.ackedBytes = 0
		p.bwWindowStart = now
		s.adaptCapLocked(p)
	}
	return true
}

func (s *Scheduler) adaptCapLocked(p *Pipe) {
	bdp := int64(p.estBW * p.ewmaRTT.Seconds() * 1.5)
	if bdp < minPipeCapBytes {
		bdp = minPipeCapBytes
	}
	if bdp > maxPipeCapBytes {
		bdp = maxPipeCapBytes
	}
	if bdp < initialPipeCapBytes && !p.hasSamples {
		bdp = initialPipeCapBytes
	}
	p.capBytes = bdp
}

func (s *Scheduler) MedianRTT() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.medianRTTLocked()
}

func (s *Scheduler) medianRTTLocked() time.Duration {
	if len(s.pipes) == 0 {
		return initialRTT
	}
	rtts := make([]time.Duration, 0, len(s.pipes))
	for _, p := range s.pipes {
		if !p.closed {
			rtts = append(rtts, p.ewmaRTT)
		}
	}
	if len(rtts) == 0 {
		return initialRTT
	}
	slices.Sort(rtts)
	return rtts[len(rtts)/2]
}

func (s *Scheduler) RescueExpired(now time.Time) []InFlightFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	baseTimeout := max(3*s.medianRTTLocked(), minRescueTimeout)
	var expired []InFlightFrame
	penalized := make(map[uint16]bool)
	for id, item := range s.inFlight {
		timeout := baseTimeout
		if p, ok := s.pipes[item.PipeID]; ok && p.inFlightBytes > 0 {
			bw := p.estBW
			if bw < minEstBW {
				bw = minEstBW
			}
			drain := time.Duration(float64(p.inFlightBytes) / bw * float64(time.Second))
			pipeExpected := p.ewmaRTT + drain
			if p.hasSamples || p.inFlightBytes >= minPipeCapBytes {
				timeout = max(timeout, min(2*pipeExpected, maxRescueTimeout))
			}
		}
		if now.Sub(item.SentAt) < timeout {
			continue
		}
		if p, ok := s.pipes[item.PipeID]; ok {
			p.inFlightBytes = max(0, p.inFlightBytes-int64(item.Size))
			penalized[item.PipeID] = true
		}
		delete(s.inFlight, id)
		item.Retries++
		expired = append(expired, *item)
	}
	for pid := range penalized {
		if p, ok := s.pipes[pid]; ok {
			p.capBytes = max(minPipeCapBytes, p.capBytes/2)
			p.ewmaRTT = min(max(p.ewmaRTT*2, 500*time.Millisecond), maxPipeRTT)
			p.stalledUntil = now.Add(3 * time.Second)
		}
	}
	return expired
}

func (s *Scheduler) RemoveStreamInFlight(streamID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, item := range s.inFlight {
		if item.Frame.StreamID == streamID {
			if p, ok := s.pipes[item.PipeID]; ok {
				p.inFlightBytes = max(0, p.inFlightBytes-int64(item.Size))
			}
			delete(s.inFlight, id)
		}
	}
}
