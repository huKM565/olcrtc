package jitsi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zarazaex69/j"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

const (
	// bridgeMaxMessageSize stays below JVB's practical 16 KiB websocket limit.
	bridgeMaxMessageSize = 16 * 1024
	bridgeOpenTimeout    = 30 * time.Second
	// sendLoop must not wait through a full reconnect because it is the only
	// consumer of both bounded queues. Old-epoch frames are stale anyway.
	jSessionWaitTimeout = 2 * time.Second

	// colibriClassEndpointMessage is the JVB bridge-channel message class
	// used for opaque raw payloads (see endpointMessage and decodeRaw).
	colibriClassEndpointMessage = "EndpointMessage"

	// bridgeBacklogHighWater bounds what may pile up below sendLoop on its
	// way out: pion's SCTP association when the bridge is the JVB's data
	// channel, the j library's 1024-message queue when it is the colibri
	// websocket. Neither pushes back on its own - DataChannel.SendText never
	// blocks, and the websocket queue is 16 MB deep - so the bounded queues
	// above this point were bounding nothing: the sender's memory was the
	// only limit. Measured on a ~5 Mbit/s SCTP bridge, six parallel
	// downloads put 85 MB into the server process in eight seconds, two
	// minutes of queue that every later frame - connect acks, pongs - sat
	// behind; each connect failed at its deadline and liveness tore the
	// session down (olcbox#23). The gauge counts bytes in flight as well as
	// bytes pending, and in flight is a congestion window's worth, so the
	// mark has to sit above what a fast path keeps in the air or it would
	// cap throughput: 512 KB, the same figure the goolom engine uses. That
	// is thirty-two full messages, under a second at 5 Mbit/s, and many poll
	// intervals at any rate a relay carries.
	// 512 KB until #15: see relayWindow, which this sits on top of.
	bridgeBacklogHighWater = 256 * 1024
	bridgeBacklogPoll      = 5 * time.Millisecond
)

var bridgeMagic = [4]byte{'O', 'L', 'R', '1'} //nolint:gochecknoglobals // wire protocol constant

func (s *Session) openBridgeWS(ctx context.Context, jSess *j.Session) error {
	return s.openBridge(ctx, jSess, "", "colibri-ws", jSess.OpenBridge)
}

func (s *Session) openBridgeSCTP(ctx context.Context, jSess *j.Session) error {
	return s.openBridge(ctx, jSess, " sctp", "sctp", jSess.WaitBridgeSCTP)
}

func (s *Session) openBridge(
	ctx context.Context,
	jSess *j.Session,
	errorSuffix string,
	transport string,
	open func(context.Context) error,
) error {
	bctx, bcancel := context.WithTimeout(ctx, bridgeOpenTimeout)
	err := open(bctx)
	bcancel()
	if err != nil {
		return fmt.Errorf("open bridge%s: %w", errorSuffix, err)
	}
	s.peerEndpoint.Store(nil)
	s.peerVideoSSRC.Store(0)
	s.markBridgeReady()
	s.requestPeerVideo(ctx, jSess)
	if br := jSess.Bridge(); br != nil {
		br.EnableRawMode()
	}
	logger.Infof("jitsi: bridge open %s (endpoints=%v)", transport, endpointList(jSess))
	return nil
}

// endpointList is the room's other occupants, for the log, and nothing at
// all for a session with no connection behind it.
//
// ai-generated: this helper.
func endpointList(jSess *j.Session) []string {
	if jSess == nil || jSess.Conn == nil {
		return nil
	}
	return jSess.Endpoints()
}

// videoMaxHeight is the height the receiver constraints ask for. The
// transports carry bytes, not pictures, so any layer will do; what matters
// is that a constraint is there at all.
const videoMaxHeight = 720

// requestPeerVideo establishes the receiver constraints on the bridge that
// was just opened. JVB forwards a participant's video only to an endpoint
// that asked for it, and the ask goes over the bridge, so every bridge needs
// it: the first one and the one a rejoin opens. Without it a rejoin came up
// with a live conference, a live bridge and no video at all, and the tunnel
// never carried a byte again (#9).
//
// ai-generated: this method and its call in openBridge.
func (s *Session) requestPeerVideo(ctx context.Context, jSess *j.Session) {
	if !s.shouldRequestVideo() {
		return
	}
	ask := s.askVideo
	if ask == nil {
		ask = func(ctx context.Context, sess *j.Session) error {
			return sess.RequestVideo(ctx, videoMaxHeight)
		}
	}
	if err := ask(ctx, jSess); err != nil {
		logger.Debugf("jitsi: request video: %v", err)
	}
}

// peerLatchFresh is how long the latched endpoint is trusted as a
// destination after the last frame from it. The control plane speaks every
// control.DefaultInterval (10 s), so three intervals of silence means
// something is wrong with the peer, not with the clock - and the old
// behaviour, a frame the bridge hands to the whole room, is the safe thing
// to fall back to while the session sorts itself out.
const peerLatchFresh = 30 * time.Second

// broadcastTarget is where a frame that belongs to no peer queue goes.
//
// On the server it is the room: the server has many clients and its
// broadcasts are for all of them. On a client it is the one endpoint it has
// latched - the server's - because the bridge hands a frame addressed to ""
// to every endpoint in the room, so each client's upload also lands in every
// other client's receive queue on the bridge, where it is dropped after
// taking up room in a buffer of about 2 MB (#25). The server never latches
// an endpoint (it takes the peer path instead), so it is unaffected.
//
// ai-generated: the whole function.
func (s *Session) broadcastTarget() string {
	ep := s.peerEndpoint.Load()
	if ep == nil {
		return ""
	}
	if seen := s.peerEndpointSeen.Load(); seen == 0 ||
		time.Since(time.Unix(0, seen)) > peerLatchFresh {
		return ""
	}
	return *ep
}

// Send queues a broadcast bridge frame, waiting for room in the queue.
func (s *Session) Send(data []byte) error {
	if s.closed.Load() {
		return ErrSessionClosed
	}
	if !s.bridgeReady.Load() {
		return ErrBridgeNotReady
	}
	framed, err := s.encodeBridgeFrame(data, "")
	if err != nil {
		return err
	}
	return s.enqueueBridgeFrame(framed)
}

// SendTo queues a bridge frame for a specific Jitsi endpoint.
func (s *Session) SendTo(peerID string, data []byte) error {
	if peerID == "" {
		return s.Send(data)
	}
	if s.closed.Load() {
		return ErrSessionClosed
	}
	if !s.bridgeReady.Load() {
		return ErrBridgeNotReady
	}
	framed, err := s.encodeBridgeFrame(data, peerID)
	if err != nil {
		return err
	}
	return s.enqueuePeerBridgeFrame(peerID, framed)
}

// enqueueBridgeFrame queues a broadcast frame. A full queue is back-pressure
// for smux, not a failure: the writer slows down, the session stays up, and
// a control frame behind it waits at most one queue's worth.
func (s *Session) enqueueBridgeFrame(framed []byte) error {
	if len(framed) > bridgeMaxMessageSize {
		return ErrSendTooLarge
	}
	select {
	case s.sendQueue <- framed:
		return nil
	case <-s.done:
		return ErrSessionClosed
	}
}

// enqueuePeerBridgeFrame queues a frame on the peer's own queue and wakes
// the sender.
func (s *Session) enqueuePeerBridgeFrame(peerID string, framed []byte) error {
	if len(framed) > bridgeMaxMessageSize {
		return ErrSendTooLarge
	}
	pq := s.peerQueueFor(peerID)
	defer s.releasePeerQueue(pq)
	select {
	case pq.ch <- framed:
		s.wakePeerSender()
		return nil
	case <-s.done:
		return ErrSessionClosed
	}
}

// peerQueueFor returns the peer's queue with a reference held; the caller
// releases it once its frame is in (or the session is gone).
func (s *Session) peerQueueFor(peerID string) *peerQueue {
	s.peerQueueMu.Lock()
	defer s.peerQueueMu.Unlock()
	pq := s.peerQueues[peerID]
	if pq == nil {
		pq = &peerQueue{ch: make(chan []byte, defaultSendQueueSize)}
		s.peerQueues[peerID] = pq
	}
	pq.refs++
	pq.lastUsed = time.Now()
	return pq
}

func (s *Session) releasePeerQueue(pq *peerQueue) {
	s.peerQueueMu.Lock()
	pq.refs--
	s.peerQueueMu.Unlock()
}

func (s *Session) wakePeerSender() {
	select {
	case s.peerWake <- struct{}{}:
	default:
	}
}

func (s *Session) sendLoop() {
	// ai-generated: the retry timer, for peers the relay window holds.
	var retry <-chan time.Time
	for {
		select {
		case <-s.done:
			return
		case data, ok := <-s.sendQueue:
			if !ok {
				return
			}
			s.sendBridgeFrame(s.broadcastTarget(), data)
		case <-s.peerWake:
			retry = s.drainPeerQueuesRetry()
		case <-retry:
			retry = s.drainPeerQueuesRetry()
		}
	}
}

// drainPeerQueuesRetry drains the peer queues and, when the relay window held
// some back, says when to look again: probes and the dead check run on this
// side, so a held peer needs a visit even when nothing wakes the loop.
//
// ai-generated: added for the relay window (olcrtc#15).
func (s *Session) drainPeerQueuesRetry() <-chan time.Time {
	if !s.drainPeerQueues() {
		return nil
	}
	return time.After(s.relayTiming.retryAfter())
}

// drainPeerQueues sends one frame per peer per pass, round-robin, until
// every peer queue is empty or held by its relay window, serving the
// broadcast queue between peers so a busy room does not starve it. A peer
// the window holds keeps its frames and is skipped, so one stalled receiver
// does not hold the others. Queues nobody has touched for peerQueueIdle and
// nobody holds are dropped at the end. Reports whether a window held any.
func (s *Session) drainPeerQueues() bool {
	defer func() {
		clear(s.drainPeers)
		clear(s.drainQueues)
		s.drainPeers = s.drainPeers[:0]
		s.drainQueues = s.drainQueues[:0]
	}()
	for {
		s.peerQueueMu.Lock()
		s.drainPeers = s.drainPeers[:0]
		s.drainQueues = s.drainQueues[:0]
		for id, pq := range s.peerQueues {
			s.drainPeers = append(s.drainPeers, id)
			s.drainQueues = append(s.drainQueues, pq)
		}
		s.peerQueueMu.Unlock()

		progressed, held := false, false
		for i, pq := range s.drainQueues {
			select {
			case <-s.done:
				return false
			default:
			}
			if len(pq.ch) > 0 && !s.relayReady(s.drainPeers[i]) {
				held = true
			} else {
				select {
				case data := <-pq.ch:
					s.sendBridgeFrame(s.drainPeers[i], data)
					progressed = true
				default:
				}
			}
			select {
			case data := <-s.sendQueue:
				s.sendBridgeFrame(s.broadcastTarget(), data)
			default:
			}
		}
		if !progressed {
			s.reapPeerQueues()
			return held
		}
	}
}

func (s *Session) reapPeerQueues() {
	cutoff := time.Now().Add(-peerQueueIdle)
	s.peerQueueMu.Lock()
	defer s.peerQueueMu.Unlock()
	for id, pq := range s.peerQueues {
		if pq.refs == 0 && len(pq.ch) == 0 && pq.lastUsed.Before(cutoff) {
			delete(s.peerQueues, id)
			// ai-generated: the peer's relay window goes with its queue.
			s.relayMu.Lock()
			delete(s.relayWin, id)
			s.relayMu.Unlock()
		}
	}
}

func (s *Session) CopiesSend() bool { return true }

func (s *Session) sendBridgeFrame(to string, data []byte) {
	if s.sendHook == nil {
		defer releaseBridgeFrame(data)
	}
	if !s.outboundFrameCurrent(data) {
		return
	}
	send := s.bridgeSender()
	if send == nil {
		return
	}
	// ai-generated: the relay window wait and count (olcrtc#15). Only a
	// single-peer session waits for its window here. drainPeerQueues let a
	// peer's frame through already; an echo that has turned the window on
	// since must not hold the one loop that serves every other peer. A
	// broadcast in peer mode has no window to wait for: an echo moves only
	// the window of the endpoint it came from.
	if !s.waitBridgeRoom() || (s.onPeerData == nil && !s.waitRelayRoom(to, data)) || !s.outboundFrameCurrent(data) {
		return
	}
	if err := send(to, data); err != nil {
		if s.closed.Load() {
			return
		}
		logger.Debugf("jitsi bridge send: %v", err)
		return
	}
	s.relaySent(to, len(data))
}

// bridgeSender returns what puts a frame on the bridge: the live session's,
// once there is one (waiting for a reconnect as sendLoop may), or the test
// hook.
//
// ai-generated: the seam the relay tests drive two sessions through.
func (s *Session) bridgeSender() func(to string, frame []byte) error {
	if s.sendHook != nil {
		return s.sendHook
	}
	jSess := s.waitJSession()
	if jSess == nil {
		return nil
	}
	return func(to string, frame []byte) error { return sendEndpointRaw(jSess, to, frame) }
}

// bridgeBacklog reports the bytes queued below sendLoop: the SCTP
// association's unsent data plus whatever the websocket bridge still holds.
// Both gauges read zero before the bridge exists, which is the right answer -
// there is nothing to wait behind yet.
func (s *Session) bridgeBacklog() int {
	backlog := 0
	s.pcMu.Lock()
	pc := s.pc
	s.pcMu.Unlock()
	if pc != nil {
		if sctp := pc.SCTP(); sctp != nil {
			backlog += sctp.BufferedAmount()
		}
	}
	if jSess := s.jSess.Load(); jSess != nil {
		backlog += jSess.BridgeSendQueueDepth() * bridgeMaxMessageSize
	}
	return backlog
}

// waitBridgeRoom holds the sender until the backlog below it is under the
// high-water mark, so that back-pressure reaches smux instead of memory.
// Returns false when the session closes meanwhile. A link that never drains
// is not this function's concern: the queues above fill, smux writes stall,
// and liveness tears the session down, which is what unblocks this loop.
func (s *Session) waitBridgeRoom() bool {
	gauge := s.backlogGauge
	if gauge == nil {
		gauge = s.bridgeBacklog
	}
	var held time.Time
	peak := 0
	for {
		backlog := gauge()
		if backlog <= bridgeBacklogHighWater {
			if !held.IsZero() && time.Since(held) >= time.Second {
				logger.Debugf("jitsi bridge: sender held %v behind a %d-byte backlog",
					time.Since(held).Round(time.Millisecond), peak)
			}
			return true
		}
		if held.IsZero() {
			held = time.Now()
		}
		peak = max(peak, backlog)
		select {
		case <-s.done:
			return false
		case <-time.After(bridgeBacklogPoll):
		}
	}
}

type endpointMessage struct {
	ColibriClass string             `json:"colibriClass"` //nolint:tagliatelle // JVB wire protocol uses camelCase
	To           string             `json:"to"`           //nolint:tagliatelle // JVB wire protocol uses camelCase
	MsgPayload   endpointRawPayload `json:"msgPayload"`   //nolint:tagliatelle // JVB wire protocol uses camelCase
}

type endpointRawPayload struct {
	Raw []byte `json:"raw"`
}

func sendEndpointRaw(jSess *j.Session, to string, data []byte) error {
	br := jSess.Bridge()
	if br == nil {
		return ErrBridgeNotReady
	}
	if err := br.SendJSON(newEndpointMessage(to, data)); err != nil {
		return fmt.Errorf("send endpoint message: %w", err)
	}
	return nil
}

func trySendEndpointRaw(jSess *j.Session, to string, data []byte) error {
	br := jSess.Bridge()
	if br == nil {
		return ErrBridgeNotReady
	}
	if err := br.TrySendJSON(newEndpointMessage(to, data)); err != nil {
		return fmt.Errorf("try send endpoint message: %w", err)
	}
	return nil
}

func newEndpointMessage(to string, data []byte) endpointMessage {
	if data == nil {
		data = []byte{}
	}
	return endpointMessage{
		ColibriClass: colibriClassEndpointMessage,
		To:           to,
		MsgPayload: endpointRawPayload{
			Raw: data,
		},
	}
}

// setJSession installs a session and republishes the readiness signal used by
// sendLoop. Passing nil rearms the signal for the next reconnect.
func (s *Session) setJSession(sess *j.Session) *j.Session {
	old := s.jSess.Swap(sess)

	s.jSessMu.Lock()
	defer s.jSessMu.Unlock()
	if s.jSessReady == nil {
		s.jSessReady = make(chan struct{})
	}
	if sess == nil {
		select {
		case <-s.jSessReady:
			s.jSessReady = make(chan struct{})
		default:
		}
		return old
	}
	select {
	case <-s.jSessReady:
	default:
		close(s.jSessReady)
	}
	return old
}

func (s *Session) waitJSession() *j.Session {
	if s.closed.Load() {
		return nil
	}
	if jSess := s.jSess.Load(); jSess != nil {
		return jSess
	}

	s.jSessMu.Lock()
	if s.jSessReady == nil {
		s.jSessReady = make(chan struct{})
	}
	ready := s.jSessReady
	s.jSessMu.Unlock()

	timer := time.NewTimer(jSessionWaitTimeout)
	defer timer.Stop()
	select {
	case <-ready:
		return s.jSess.Load()
	case <-s.done:
		return nil
	case <-timer.C:
		return nil
	}
}

// recvLoop consumes the bridge channel. Only one instance may run at a time:
// Connect, completeJingleSetup and finishReconnect each start one, and two
// loops racing on the same channel split frames between them and hand them to
// onData concurrently and out of order - which the record layer's replay
// window then rejects as junk. A later loop waits here until the previous one
// has seen its channel close.
func (s *Session) recvLoop() {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()

	gen := s.bridgeGen.Load()
	jSess := s.jSess.Load()
	if jSess == nil || (s.onData == nil && s.onPeerData == nil) || !s.bridgeReady.Load() {
		logger.Debugf("jitsi: recvLoop early exit jSess=%v onData=%v onPeerData=%v bridgeReady=%v",
			jSess != nil, s.onData != nil, s.onPeerData != nil, s.bridgeReady.Load())
		return
	}
	msgs := jSess.BridgeMessages()
	if msgs == nil {
		logger.Debugf("jitsi: recvLoop: BridgeMessages() returned nil, exiting")
		return
	}
	br := jSess.Bridge()
	var rawMsgs <-chan []byte
	if br != nil {
		rawMsgs = br.RawFrames()
	}
	logger.Debugf("jitsi: recvLoop started")
drainLoop:
	for {
		select {
		case msg, ok := <-msgs:
			if !s.deliverBridgeMessageGen(gen, msg, ok) {
				return
			}
		default:
			break drainLoop
		}
	}
	for {
		select {
		case <-s.done:
			return
		case msg, ok := <-msgs:
			if !s.deliverBridgeMessageGen(gen, msg, ok) {
				return
			}
		case rawJSON, ok := <-rawMsgs:
			if !ok {
				s.deliverBridgeMessageGen(gen, j.BridgeMessage{}, false)
				return
			}
			if !s.deliverRawBridgeFrameGen(gen, rawJSON) {
				return
			}
		}
	}
}

func (s *Session) deliverBridgeMessage(msg j.BridgeMessage, ok bool) bool {
	return s.deliverBridgeMessageGen(s.bridgeGen.Load(), msg, ok)
}

func (s *Session) deliverBridgeMessageGen(gen uint64, msg j.BridgeMessage, ok bool) bool {
	if !ok {
		if !s.closed.Load() {
			s.requestReconnectGen(gen, "jitsi bridge closed")
		}
		return false
	}
	return s.deliverDecodedBridgeFrame(msg.From, decodeRaw(msg))
}

func (s *Session) deliverRawBridgeFrameGen(_ uint64, rawJSON []byte) bool {
	from, raw, ok := decodeRawJSON(rawJSON)
	if !ok {
		return true
	}
	return s.deliverDecodedBridgeFrame(from, raw)
}

func (s *Session) deliverDecodedBridgeFrame(from string, raw []byte) bool {
	// ai-generated: marks and echoes of the relay window (olcrtc#15).
	if s.handleWindowFrame(from, raw) {
		return true
	}
	payload, valid := bridgePayload(raw)
	if !valid {
		return true
	}
	if s.onPeerData != nil && from != "" {
		return s.deliverPeerBridgePayload(from, payload)
	}
	data, accepted := s.acceptEpochFrame(payload)
	if !accepted {
		return true
	}
	if !s.requireTargetedPeer || s.peerEpoch.Load() != 0 {
		s.latchPeerEndpoint(from)
	}
	if len(data) == 0 {
		return true
	}
	s.onData(data)
	return true
}

// bridgePayload returns a decoded payload that carries the data magic.
//
// ai-generated: it takes the payload deliverBridgeMessageGen decoded once
// for the relay window's frames, not the message (olcrtc#15).
func bridgePayload(payload []byte) ([]byte, bool) {
	if payload == nil {
		return nil, false
	}
	if len(payload) < len(bridgeMagic) || !bytes.Equal(payload[:len(bridgeMagic)], bridgeMagic[:]) {
		return nil, false
	}
	return payload, true
}

func (s *Session) deliverPeerBridgePayload(from string, payload []byte) bool {
	data, ok := s.acceptPeerEpochFrame(from, payload)
	if !ok || len(data) == 0 {
		return true
	}
	s.onPeerData(from, data)
	return true
}

func decodeRawJSON(data []byte) (string, []byte, bool) {
	classVal, fromVal, topRaw, msgRaw, hasTopRaw, hasMsgRaw, escaped, ok := scanEndpointMessageJSON(data)
	var from string
	var b64 []byte
	var hasRaw bool
	if escaped || !ok {
		var fb struct {
			ColibriClass string `json:"colibriClass"`
			From         string `json:"from"`
			Raw          string `json:"raw"`
			MsgPayload   *struct {
				Raw *string `json:"raw"`
			} `json:"msgPayload"`
		}
		if err := json.Unmarshal(data, &fb); err != nil {
			return "", nil, false
		}
		if fb.ColibriClass != colibriClassEndpointMessage {
			return "", nil, false
		}
		from = fb.From
		if fb.MsgPayload != nil && fb.MsgPayload.Raw != nil {
			b64 = []byte(*fb.MsgPayload.Raw)
			hasRaw = true
		} else if fb.Raw != "" {
			b64 = []byte(fb.Raw)
			hasRaw = true
		}
	} else {
		if !bytes.Equal(classVal, []byte(colibriClassEndpointMessage)) {
			return "", nil, false
		}
		switch {
		case hasMsgRaw:
			b64 = msgRaw
			hasRaw = true
		case hasTopRaw:
			b64 = topRaw
			hasRaw = true
		}
		if len(fromVal) > 0 {
			from = string(fromVal)
		}
	}
	if !hasRaw {
		return "", nil, false
	}
	out := make([]byte, base64.StdEncoding.DecodedLen(len(b64)))
	n, err := base64.StdEncoding.Decode(out, b64)
	if err != nil {
		return "", nil, false
	}
	return from, out[:n], true
}

func scanEndpointMessageJSON(data []byte) (classVal, fromVal, topRaw, msgRaw []byte, hasTopRaw, hasMsgRaw, escaped, ok bool) {
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return nil, nil, nil, nil, false, false, false, false
	}
	i = skipJSONSpace(data, i+1)
	if i < len(data) && data[i] == '}' {
		return nil, nil, nil, nil, false, false, false, true
	}
	for i < len(data) {
		key, next, keyEsc, keyOK := scanJSONString(data, i)
		if !keyOK {
			return nil, nil, nil, nil, false, false, true, false
		}
		if keyEsc {
			return nil, nil, nil, nil, false, false, true, true
		}
		i = skipJSONSpace(data, next)
		if i >= len(data) || data[i] != ':' {
			return nil, nil, nil, nil, false, false, false, false
		}
		i = skipJSONSpace(data, i+1)
		if i >= len(data) {
			return nil, nil, nil, nil, false, false, false, false
		}
		switch string(key) {
		case "colibriClass", "from", "raw":
			if data[i] == '"' {
				val, end, valEsc, valOK := scanJSONString(data, i)
				if !valOK {
					return nil, nil, nil, nil, false, false, true, false
				}
				if valEsc {
					return nil, nil, nil, nil, false, false, true, true
				}
				switch string(key) {
				case "colibriClass":
					classVal = val
				case "from":
					fromVal = val
				case "raw":
					topRaw = val
					hasTopRaw = true
				}
				i = end
			} else {
				end, skipOK := skipJSONValue(data, i)
				if !skipOK {
					return nil, nil, nil, nil, false, false, false, false
				}
				i = end
			}
		case "msgPayload":
			if data[i] == '{' {
				rawVal, foundRaw, payloadEsc, end, payloadOK := scanMsgPayloadRaw(data, i)
				if !payloadOK {
					return nil, nil, nil, nil, false, false, payloadEsc, false
				}
				if payloadEsc {
					return nil, nil, nil, nil, false, false, true, true
				}
				if foundRaw {
					msgRaw = rawVal
					hasMsgRaw = true
				}
				i = end
			} else {
				end, skipOK := skipJSONValue(data, i)
				if !skipOK {
					return nil, nil, nil, nil, false, false, false, false
				}
				i = end
			}
		default:
			end, skipOK := skipJSONValue(data, i)
			if !skipOK {
				return nil, nil, nil, nil, false, false, false, false
			}
			i = end
		}
		i = skipJSONSpace(data, i)
		if i >= len(data) {
			return nil, nil, nil, nil, false, false, false, false
		}
		if data[i] == ',' {
			i = skipJSONSpace(data, i+1)
			continue
		}
		if data[i] == '}' {
			return classVal, fromVal, topRaw, msgRaw, hasTopRaw, hasMsgRaw, false, true
		}
		return nil, nil, nil, nil, false, false, false, false
	}
	return nil, nil, nil, nil, false, false, false, false
}

func scanMsgPayloadRaw(data []byte, start int) (rawVal []byte, hasRaw, escaped bool, next int, ok bool) {
	i := skipJSONSpace(data, start+1)
	if i < len(data) && data[i] == '}' {
		return nil, false, false, i + 1, true
	}
	for i < len(data) {
		key, afterKey, keyEsc, keyOK := scanJSONString(data, i)
		if !keyOK {
			return nil, false, true, 0, false
		}
		if keyEsc {
			return nil, false, true, 0, true
		}
		i = skipJSONSpace(data, afterKey)
		if i >= len(data) || data[i] != ':' {
			return nil, false, false, 0, false
		}
		i = skipJSONSpace(data, i+1)
		if i >= len(data) {
			return nil, false, false, 0, false
		}
		if string(key) == "raw" && data[i] == '"' {
			val, end, valEsc, valOK := scanJSONString(data, i)
			if !valOK {
				return nil, false, true, 0, false
			}
			if valEsc {
				return nil, false, true, 0, true
			}
			rawVal = val
			hasRaw = true
			i = end
		} else {
			end, skipOK := skipJSONValue(data, i)
			if !skipOK {
				return nil, false, false, 0, false
			}
			i = end
		}
		i = skipJSONSpace(data, i)
		if i >= len(data) {
			return nil, false, false, 0, false
		}
		if data[i] == ',' {
			i = skipJSONSpace(data, i+1)
			continue
		}
		if data[i] == '}' {
			return rawVal, hasRaw, false, i + 1, true
		}
		return nil, false, false, 0, false
	}
	return nil, false, false, 0, false
}

func skipJSONSpace(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\r', '\n':
			i++
		default:
			return i
		}
	}
	return i
}

func scanJSONString(data []byte, start int) (val []byte, next int, escaped, ok bool) {
	if start >= len(data) || data[start] != '"' {
		return nil, 0, false, false
	}
	i := start + 1
	for i < len(data) {
		q := bytes.IndexByte(data[i:], '"')
		if q < 0 {
			return nil, 0, escaped, false
		}
		bs := bytes.IndexByte(data[i:i+q], '\\')
		if bs < 0 {
			return data[start+1 : i+q], i + q + 1, escaped, true
		}
		escaped = true
		i += bs + 2
	}
	return nil, 0, escaped, false
}

func skipJSONValue(data []byte, start int) (int, bool) {
	if start >= len(data) {
		return 0, false
	}
	switch data[start] {
	case '"':
		_, next, _, ok := scanJSONString(data, start)
		return next, ok
	case '{', '[':
		depth := 0
		inString := false
		for i := start; i < len(data); i++ {
			c := data[i]
			if inString {
				if c == '\\' {
					i++
					continue
				}
				if c == '"' {
					inString = false
				}
				continue
			}
			switch c {
			case '"':
				inString = true
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1, true
				}
			}
		}
		return 0, false
	default:
		i := start
		for i < len(data) {
			switch data[i] {
			case ',', '}', ']', ' ', '\t', '\r', '\n':
				if i == start {
					return 0, false
				}
				return i, true
			}
			i++
		}
		return i, i > start
	}
}

// decodeRaw extracts the base64 payload from an EndpointMessage. It accepts
// both the standard msgPayload.raw shape (as sent by sendEndpointRaw) and the
// legacy top-level "raw" field (as sent by older olcrtc builds, or by peers
// still on the j library's BridgeSendRaw), for backward compatibility. See
// olcrtc#143.
func decodeRaw(m j.BridgeMessage) []byte {
	if m.Class != colibriClassEndpointMessage {
		return nil
	}
	enc, ok := rawFieldFrom(m.Fields)
	if !ok {
		return nil
	}
	out, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil
	}
	return out
}

func rawFieldFrom(fields map[string]any) (string, bool) {
	if payload, ok := fields["msgPayload"].(map[string]any); ok {
		if raw, ok := payload["raw"].(string); ok {
			return raw, true
		}
	}
	raw, ok := fields["raw"].(string)
	return raw, ok
}

func (s *Session) markBridgeReady() {
	s.bridgeGen.Add(1)
	s.bridgeReady.Store(true)
}
