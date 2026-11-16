package jitsi

import (
	"sync"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// occupancyPollInterval is how often a live Jitsi session reads its MUC roster.
// ai-generated: the room occupancy reporter and watcher in this file.
const occupancyPollInterval = 2 * time.Second

type occupancyReporter struct {
	mu       sync.Mutex
	last     int
	onChange func(participants int)
}

func (r *occupancyReporter) report(participants int) {
	r.mu.Lock()
	if r.last == participants {
		r.mu.Unlock()
		return
	}
	r.last = participants
	r.mu.Unlock()

	logger.Infof("jitsi: room participants=%d", participants)
	if r.onChange != nil {
		r.onChange(participants)
	}
}

func roomParticipants(endpoints []string) int {
	return len(endpoints) + 1
}

func (s *Session) reportOccupancy() {
	jSess := s.jSess.Load()
	if jSess == nil || jSess.Conn == nil {
		s.occupancy.report(0)
		return
	}
	s.occupancy.report(roomParticipants(endpointList(jSess)))
}

func (s *Session) watchOccupancy() {
	ticker := time.NewTicker(occupancyPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.reportOccupancy()
		}
	}
}
