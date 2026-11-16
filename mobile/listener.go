package mobile

// SessionListener receives tunnel session events from a Runtime.
//
// It is the mobile counterpart of the client's "session <id> opened" log
// line: the moment a session is established in a room. A gomobile host cannot
// read the runtime's log, so the event arrives as a call and names the room,
// which is what lets the host tell a reconnect within the room it was in from
// a failover to another one - and act on the latter, for instance by
// refreshing its room list through the new session.
//
// ai-generated: the whole file.
type SessionListener interface {
	// OnSessionOpened reports one established session: on the initial
	// connect, after a reconnect within the same room, and after a failover.
	// room is the room the session is in, as given to SetRoom or
	// AddFailoverRoom; sessionID is the server-assigned id. It is called on
	// the runtime's connect path, under the client's reconnect lock: return
	// promptly and do the work elsewhere, or the next outage waits for it.
	OnSessionOpened(room string, sessionID string)
}

// SetSessionListener installs the listener for this Runtime, replacing any
// previous one; nil removes it. Safe to call while a generation is live: the
// next event goes to the new listener.
func (r *Runtime) SetSessionListener(l SessionListener) {
	r.mu.Lock()
	r.listener = l
	r.mu.Unlock()
}

// notifySessionOpened forwards a session event to the listener, unless the
// generation it came from is already on its way out: a session that opens
// while its generation is being stopped is not one the host should act on.
func (r *Runtime) notifySessionOpened(gen *runGeneration, room, sessionID string) {
	r.mu.Lock()
	listener := r.listener
	live := r.isCurrentGenerationLocked(gen) && !gen.stopRequested
	r.mu.Unlock()
	if listener == nil || !live {
		return
	}
	listener.OnSessionOpened(room, sessionID)
}

// OccupancyListener receives the current number of participants in a Jitsi room.
// The count includes the local endpoint and excludes the service focus user.
// ai-generated: the room occupancy listener interface and delivery below.
type OccupancyListener interface {
	OnRoomOccupancyChanged(room string, participants int)
}

// SetOccupancyListener installs the roster listener for this Runtime.
func (r *Runtime) SetOccupancyListener(l OccupancyListener) {
	r.mu.Lock()
	r.occupancyListener = l
	r.mu.Unlock()
}

// notifyRoomOccupancy drops events from a generation that is already stopping.
func (r *Runtime) notifyRoomOccupancy(gen *runGeneration, room string, participants int) {
	r.mu.Lock()
	listener := r.occupancyListener
	live := r.isCurrentGenerationLocked(gen) && !gen.stopRequested
	if live {
		gen.occupancyRoom = room
		gen.occupancyParticipants = participants
		gen.occupancyKnown = true
	}
	r.mu.Unlock()
	if listener == nil || !live {
		return
	}
	listener.OnRoomOccupancyChanged(room, participants)
}

// RequestRoomOccupancy replays the latest roster snapshot of the running generation.
// ai-generated: cached occupancy replay for hosts that activate a room after its first event.
func (r *Runtime) RequestRoomOccupancy() {
	r.mu.Lock()
	gen := r.current
	if gen == nil || r.state != stateRunning || gen.stopRequested || !gen.occupancyKnown {
		r.mu.Unlock()
		return
	}
	listener := r.occupancyListener
	room := gen.occupancyRoom
	participants := gen.occupancyParticipants
	r.mu.Unlock()
	if listener != nil {
		listener.OnRoomOccupancyChanged(room, participants)
	}
}

// RoomStateListener receives provider room entry and relay establishment events.
// ai-generated: room state listener, current-generation cache and replay.
type RoomStateListener interface {
	OnRoomStateChanged(room string, state string)
}

// SetRoomStateListener installs a listener and replays the latest state of the current generation.
func (r *Runtime) SetRoomStateListener(l RoomStateListener) {
	r.mu.Lock()
	r.roomStateListener = l
	r.mu.Unlock()
	r.RequestRoomState()
}

func (r *Runtime) notifyRoomState(gen *runGeneration, room, state string) {
	r.mu.Lock()
	listener := r.roomStateListener
	live := r.isCurrentGenerationLocked(gen) && !gen.stopRequested
	if live {
		gen.roomStateRoom = room
		gen.roomState = state
		gen.roomStateKnown = true
	}
	r.mu.Unlock()
	if listener != nil && live {
		listener.OnRoomStateChanged(room, state)
	}
}

// RequestRoomState replays only the latest state of the current live generation.
func (r *Runtime) RequestRoomState() {
	r.mu.Lock()
	gen := r.current
	if gen == nil || !r.isCurrentGenerationLocked(gen) || gen.stopRequested || !gen.roomStateKnown {
		r.mu.Unlock()
		return
	}
	listener := r.roomStateListener
	room := gen.roomStateRoom
	state := gen.roomState
	r.mu.Unlock()
	if listener != nil {
		listener.OnRoomStateChanged(room, state)
	}
}
