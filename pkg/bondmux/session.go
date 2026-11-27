package bondmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ackFlushInterval       = 10 * time.Millisecond
	ackBatchTrigger        = 4
	pingInterval           = 2 * time.Second
	pipeIdleTimeout        = 12 * time.Second
	rescueTickInterval     = 100 * time.Millisecond
	windowRefreshInterval  = 250 * time.Millisecond
	zeroPipeStreamWait     = 15 * time.Second
	openStreamTimeout      = 15 * time.Second
)

var (
	ErrSessionClosed   = errors.New("bondmux: session closed")
	ErrStreamReset     = errors.New("bondmux: stream reset")
	ErrMaxStreams      = errors.New("bondmux: max concurrent streams reached")
	ErrNoPipes         = errors.New("bondmux: no healthy pipes available")
)

type pingEntry struct {
	pipeID uint16
	sentAt time.Time
}

type Session struct {
	id            [16]byte
	isServer      bool
	chunkSize     int
	streamWindow  uint32
	sessionWindow int64
	maxStreams    int
	logf          func(string, ...any)

	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool

	sched        *Scheduler
	sessionBytes atomic.Int64
	nextStreamID atomic.Uint32
	nextChunkID  atomic.Uint32
	nextNonce    atomic.Uint64

	streamsMu sync.RWMutex
	streams   map[uint32]*muxStream

	ackMu       sync.Mutex
	pendingAcks []uint32
	ackWake     chan struct{}

	pingMu   sync.Mutex
	pings    map[uint64]pingEntry
	pipeLast sync.Map // map[uint16]int64 (unix nano)

	schedWake chan struct{}

	onOpenTCP func(stream *muxStream, dst TargetAddr)
	onUDP     func(flowID uint32, dst TargetAddr, data []byte)
}

func newSession(
	parent context.Context,
	id [16]byte,
	isServer bool,
	chunkSize int,
	streamWindow uint32,
	sessionWindow uint32,
	maxStreams int,
	logf func(string, ...any),
) *Session {
	if chunkSize <= 0 || chunkSize > MaxPayload {
		chunkSize = DefaultChunkSize
	}
	if streamWindow == 0 {
		streamWindow = DefaultStreamWindowDesktop
	}
	if sessionWindow == 0 {
		sessionWindow = DefaultSessionWindowDesktop
	}
	if maxStreams <= 0 {
		maxStreams = DefaultMaxStreamsDesktop
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	ctx, cancel := context.WithCancel(parent)
	s := &Session{
		id:            id,
		isServer:      isServer,
		chunkSize:     chunkSize,
		streamWindow:  streamWindow,
		sessionWindow: int64(sessionWindow),
		maxStreams:    maxStreams,
		logf:          logf,
		ctx:           ctx,
		cancel:        cancel,
		sched:         NewScheduler(nil),
		streams:       make(map[uint32]*muxStream),
		ackWake:       make(chan struct{}, 1),
		pings:         make(map[uint64]pingEntry),
		schedWake:     make(chan struct{}, 1),
	}
	if !isServer {
		s.nextStreamID.Store(1)
	}
	go s.ackLoop()
	go s.rescueLoop()
	go s.pingLoop()
	go s.windowRefreshLoop()
	return s
}

func (s *Session) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.cancel()
	for _, p := range s.sched.PipesSnapshot() {
		if p.Writer != nil {
			p.Writer.Close()
		}
		s.sched.RemovePipe(p.ID)
	}
	s.streamsMu.Lock()
	streams := make([]*muxStream, 0, len(s.streams))
	for _, st := range s.streams {
		streams = append(streams, st)
	}
	s.streams = make(map[uint32]*muxStream)
	s.streamsMu.Unlock()
	for _, st := range streams {
		st.abort(ErrSessionClosed)
	}
	s.wakeSched()
}

func (s *Session) ActivePipes() int {
	return s.sched.PipeCount()
}

func (s *Session) wakeSched() {
	select {
	case s.schedWake <- struct{}{}:
	default:
	}
}

func (s *Session) AttachPipe(pipeID uint16, conn net.Conn, onClosed func()) {
	if s.closed.Load() {
		_ = conn.Close()
		if onClosed != nil {
			onClosed()
		}
		return
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	p := NewPipe(pipeID)
	pw := NewPipeWriter(conn, func(error) {
		_ = conn.Close()
	})
	p.Writer = pw
	s.pipeLast.Store(pipeID, time.Now().UnixNano())
	s.sched.RegisterPipe(p)
	s.wakeSched()

	// Refresh stream windows after pipe topology change
	s.refreshAllWindows()

	defer func() {
		pw.Close()
		s.pipeLast.Delete(pipeID)
		orphaned := s.sched.RemovePipe(pipeID)
		s.requeueFrames(orphaned, pipeID)
		s.wakeSched()
		if onClosed != nil {
			onClosed()
		}
	}()

	for {
		f, err := ReadFrame(conn)
		if err != nil {
			return
		}
		s.pipeLast.Store(pipeID, time.Now().UnixNano())
		s.handleFrame(pipeID, f)
	}
}

func (s *Session) handleFrame(pipeID uint16, f Frame) {
	switch f.Type {
	case FramePing:
		if p, ok := s.pipeByID(pipeID); ok && p.Writer != nil {
			_ = p.Writer.TryEnqueueCtrl(Frame{Type: FramePong, Offset: f.Offset})
		}
	case FramePong:
		s.pingMu.Lock()
		ent, ok := s.pings[f.Offset]
		if ok {
			delete(s.pings, f.Offset)
		}
		s.pingMu.Unlock()
		if ok {
			s.sched.UpdatePipeRTT(ent.pipeID, time.Since(ent.sentAt))
		}
	case FrameAck:
		ids, err := DecodeAck(f.Payload)
		if err != nil {
			return
		}
		now := time.Now()
		anyAck := false
		for _, id := range ids {
			if s.sched.OnAck(id, now) {
				anyAck = true
			}
		}
		if anyAck {
			s.wakeSched()
		}
	case FrameOpenTCP:
		s.queueAck(f.ChunkID)
		if !s.isServer || s.onOpenTCP == nil {
			return
		}
		peerWin, dst, err := DecodeOpenTCP(f.Payload)
		if err != nil {
			return
		}
		s.streamsMu.Lock()
		if _, exists := s.streams[f.StreamID]; exists {
			s.streamsMu.Unlock()
			return
		}
		if len(s.streams) >= s.maxStreams {
			s.streamsMu.Unlock()
			sid := f.StreamID
			go func() {
				_ = s.sendReliableAsync(Frame{
					Type:     FrameOpenOK,
					Flags:    FlagOpenFail,
					StreamID: sid,
					Payload:  EncodeOpenOK(0, "max streams exceeded"),
				})
			}()
			return
		}
		if peerWin == 0 {
			peerWin = DefaultStreamWindowMobile
		}
		st := newMuxStream(s, f.StreamID, peerWin)
		s.streams[f.StreamID] = st
		s.streamsMu.Unlock()
		go s.onOpenTCP(st, dst)
	case FrameOpenOK:
		s.queueAck(f.ChunkID)
		s.streamsMu.RLock()
		st := s.streams[f.StreamID]
		s.streamsMu.RUnlock()
		if st == nil {
			return
		}
		peerWin, errMsg, err := DecodeOpenOK(f.Payload)
		if err != nil {
			st.resolveOpen(0, err)
			return
		}
		if f.Flags&FlagOpenFail != 0 {
			if errMsg == "" {
				errMsg = "remote dial failed"
			}
			st.resolveOpen(0, errors.New(errMsg))
			return
		}
		st.resolveOpen(peerWin, nil)
	case FrameData:
		s.streamsMu.RLock()
		st := s.streams[f.StreamID]
		s.streamsMu.RUnlock()
		if st == nil {
			s.queueAck(f.ChunkID)
			if p, ok := s.sched.FastestPipe(); ok && p.Writer != nil {
				_ = p.Writer.TryEnqueueCtrl(Frame{Type: FrameRst, StreamID: f.StreamID})
			}
			return
		}
		if st.onData(f.Offset, f.Payload) {
			s.queueAck(f.ChunkID)
		}
	case FrameWindow:
		s.streamsMu.RLock()
		st := s.streams[f.StreamID]
		s.streamsMu.RUnlock()
		if st != nil {
			st.updateSendWindow(f.Offset)
		}
	case FrameFin:
		s.queueAck(f.ChunkID)
		s.streamsMu.RLock()
		st := s.streams[f.StreamID]
		s.streamsMu.RUnlock()
		if st != nil {
			st.onFin(f.Offset)
		}
	case FrameRst:
		s.streamsMu.Lock()
		st := s.streams[f.StreamID]
		delete(s.streams, f.StreamID)
		s.streamsMu.Unlock()
		s.sched.RemoveStreamInFlight(f.StreamID)
		if st != nil {
			st.abort(ErrStreamReset)
		}
	case FrameUDPPkt:
		if s.onUDP == nil {
			return
		}
		dst, data, err := DecodeUDPPkt(f.Payload)
		if err != nil {
			return
		}
		s.onUDP(f.StreamID, dst, data)
	}
}

func (s *Session) pipeByID(id uint16) (*Pipe, bool) {
	for _, p := range s.sched.PipesSnapshot() {
		if p.ID == id {
			return p, true
		}
	}
	return nil, false
}

func (s *Session) queueAck(chunkID uint32) {
	if chunkID == 0 {
		return
	}
	s.ackMu.Lock()
	s.pendingAcks = append(s.pendingAcks, chunkID)
	shouldWake := len(s.pendingAcks) >= ackBatchTrigger
	s.ackMu.Unlock()
	if shouldWake {
		select {
		case s.ackWake <- struct{}{}:
		default:
		}
	}
}

func (s *Session) ackLoop() {
	ticker := time.NewTicker(ackFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.ackWake:
		case <-ticker.C:
		}
		s.flushAcks()
	}
}

func (s *Session) flushAcks() {
	for {
		s.ackMu.Lock()
		if len(s.pendingAcks) == 0 {
			s.ackMu.Unlock()
			return
		}
		n := min(len(s.pendingAcks), 64)
		batch := append([]uint32(nil), s.pendingAcks[:n]...)
		s.pendingAcks = s.pendingAcks[n:]
		s.ackMu.Unlock()

		p, ok := s.sched.FastestPipe()
		if !ok || p.Writer == nil {
			return
		}
		_ = p.Writer.TryEnqueueCtrl(Frame{
			Type:    FrameAck,
			Payload: EncodeAck(batch),
		})
	}
}

func (s *Session) pingLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			pipes := s.sched.PipesSnapshot()
			for _, p := range pipes {
				if lastNano, ok := s.pipeLast.Load(p.ID); ok {
					if now.Sub(time.Unix(0, lastNano.(int64))) > pipeIdleTimeout {
						if p.Writer != nil {
							p.Writer.Close()
						}
						continue
					}
				}
				nonce := s.nextNonce.Add(1)
				s.pingMu.Lock()
				s.pings[nonce] = pingEntry{pipeID: p.ID, sentAt: now}
				// Prune stale ping entries
				if len(s.pings) > 1024 {
					for k, v := range s.pings {
						if now.Sub(v.sentAt) > 15*time.Second {
							delete(s.pings, k)
						}
					}
				}
				s.pingMu.Unlock()
				if p.Writer != nil {
					_ = p.Writer.TryEnqueueCtrl(Frame{Type: FramePing, Offset: nonce})
				}
			}
		}
	}
}

func (s *Session) rescueLoop() {
	ticker := time.NewTicker(rescueTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			expired := s.sched.RescueExpired(now)
			if len(expired) > 0 {
				s.requeueFrames(expired, 0)
			}
		}
	}
}

func (s *Session) windowRefreshLoop() {
	ticker := time.NewTicker(windowRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.refreshAllWindows()
		}
	}
}

func (s *Session) refreshAllWindows() {
	s.streamsMu.RLock()
	streams := make([]*muxStream, 0, len(s.streams))
	for _, st := range s.streams {
		streams = append(streams, st)
	}
	s.streamsMu.RUnlock()
	for _, st := range streams {
		st.maybeRefreshWindow()
	}
}

func (s *Session) requeueFrames(frames []InFlightFrame, deadPipeID uint16) {
	now := time.Now()
	for _, item := range frames {
		if s.closed.Load() {
			return
		}
		if s.sched.PipeCount() == 0 {
			if !item.FirstSentAt.IsZero() && now.Sub(item.FirstSentAt) > zeroPipeStreamWait {
				s.resetStream(item.Frame.StreamID)
				continue
			}
			if item.Retries > 0 {
				item.Retries--
			}
			s.sched.TrackRetransmit(item, 0, now)
			continue
		}
		if item.Retries > maxChunkRetries {
			s.resetStream(item.Frame.StreamID)
			continue
		}
		exclude := deadPipeID != 0 || item.PipeID != 0
		exclID := item.PipeID
		if deadPipeID != 0 {
			exclID = deadPipeID
		}
		p, ok := s.sched.PickPipeExcluding(item.Size, exclID, exclude)
		if !ok {
			p, ok = s.sched.FastestPipe()
		}
		if !ok || p.Writer == nil {
			s.sched.TrackRetransmit(item, 0, now)
			continue
		}
		s.sched.TrackRetransmit(item, p.ID, now)
		if err := p.Writer.Enqueue(item.Frame); err != nil {
			continue
		}
	}
}

func (s *Session) resetStream(streamID uint32) {
	s.streamsMu.Lock()
	st := s.streams[streamID]
	delete(s.streams, streamID)
	s.streamsMu.Unlock()
	s.sched.RemoveStreamInFlight(streamID)
	if st != nil {
		st.abort(ErrStreamReset)
	}
	if p, ok := s.sched.FastestPipe(); ok && p.Writer != nil {
		_ = p.Writer.TryEnqueueCtrl(Frame{Type: FrameRst, StreamID: streamID})
	}
}

func (s *Session) sendReliable(ctx context.Context, f Frame) error {
	f.ChunkID = s.nextChunkID.Add(1)
	size := HeaderSize + len(f.Payload)
	var zeroSince time.Time
	for {
		if s.closed.Load() {
			return ErrSessionClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var p *Pipe
		var ok bool
		if f.IsHighPriority() {
			p, ok = s.sched.FastestPipe()
		} else {
			p, ok = s.sched.PickPipe(size)
		}
		if ok && p.Writer != nil {
			zeroSince = time.Time{}
			s.sched.TrackInFlight(f.ChunkID, f, p.ID, time.Now())
			if err := p.Writer.Enqueue(f); err != nil {
				// Pipe closed concurrently; rescue immediately
				s.sched.OnAck(f.ChunkID, time.Now())
				continue
			}
			return nil
		}
		if s.sched.PipeCount() == 0 {
			if zeroSince.IsZero() {
				zeroSince = time.Now()
			} else if time.Since(zeroSince) > zeroPipeStreamWait {
				return ErrNoPipes
			}
		} else {
			zeroSince = time.Time{}
		}
		t := time.NewTimer(15 * time.Millisecond)
		select {
		case <-s.ctx.Done():
			t.Stop()
			return ErrSessionClosed
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-s.schedWake:
			t.Stop()
		case <-t.C:
		}
	}
}

func (s *Session) sendReliableAsync(f Frame) error {
	ctx, cancel := context.WithTimeout(s.ctx, openStreamTimeout)
	defer cancel()
	return s.sendReliable(ctx, f)
}

func (s *Session) SendUDP(flowID uint32, dst TargetAddr, data []byte) error {
	payload, err := EncodeUDPPkt(dst, data)
	if err != nil {
		return err
	}
	f := Frame{
		Type:     FrameUDPPkt,
		StreamID: flowID,
		Payload:  payload,
	}
	p, ok := s.sched.PickPipe(HeaderSize + len(payload))
	if !ok {
		p, ok = s.sched.FastestPipe()
	}
	if !ok || p.Writer == nil {
		return ErrNoPipes
	}
	return p.Writer.Enqueue(f)
}

func (s *Session) OpenTCP(ctx context.Context, dst TargetAddr) (*muxStream, error) {
	if s.closed.Load() {
		return nil, ErrSessionClosed
	}
	payload, err := EncodeOpenTCP(s.streamWindow, dst)
	if err != nil {
		return nil, err
	}
	id := s.nextStreamID.Add(2)
	st := newMuxStream(s, id, 0)

	s.streamsMu.Lock()
	if len(s.streams) >= s.maxStreams {
		s.streamsMu.Unlock()
		return nil, ErrMaxStreams
	}
	s.streams[id] = st
	s.streamsMu.Unlock()

	openCtx, cancel := context.WithTimeout(ctx, openStreamTimeout)
	defer cancel()
	if err := s.sendReliable(openCtx, Frame{
		Type:     FrameOpenTCP,
		StreamID: id,
		Payload:  payload,
	}); err != nil {
		s.removeStream(id)
		st.abort(err)
		return nil, err
	}

	select {
	case err := <-st.openCh:
		if err != nil {
			s.removeStream(id)
			st.abort(err)
			return nil, err
		}
		return st, nil
	case <-openCtx.Done():
		s.resetStream(id)
		return nil, openCtx.Err()
	case <-s.ctx.Done():
		s.removeStream(id)
		return nil, ErrSessionClosed
	}
}

func (s *Session) removeStream(id uint32) {
	s.streamsMu.Lock()
	delete(s.streams, id)
	s.streamsMu.Unlock()
}

type muxStream struct {
	sess *Session
	id   uint32

	openOnce sync.Once
	openCh   chan error

	sendMu     sync.Mutex
	sendOffset uint64
	sendWindow uint64
	writeEOF   bool
	winWake    chan struct{}

	readMu            sync.Mutex
	rb                *ReorderBuffer
	ready             [][]byte
	readyBytes        int
	recvConsumed      uint64
	lastAdvertisedMax uint64
	readEOF           bool
	readNotify        chan struct{}

	errMu  sync.Mutex
	err    error
	closed atomic.Bool
}

func newMuxStream(sess *Session, id uint32, initialPeerWindow uint32) *muxStream {
	return &muxStream{
		sess:              sess,
		id:                id,
		openCh:            make(chan error, 1),
		sendWindow:        uint64(initialPeerWindow),
		winWake:           make(chan struct{}, 1),
		rb:                NewReorderBuffer(sess.streamWindow, &sess.sessionBytes, sess.sessionWindow),
		lastAdvertisedMax: uint64(sess.streamWindow),
		readNotify:        make(chan struct{}, 1),
	}
}

func (st *muxStream) resolveOpen(peerWin uint32, err error) {
	st.openOnce.Do(func() {
		if err == nil {
			if peerWin == 0 {
				peerWin = DefaultStreamWindowMobile
			}
			st.sendMu.Lock()
			if uint64(peerWin) > st.sendWindow {
				st.sendWindow = uint64(peerWin)
			}
			st.sendMu.Unlock()
		}
		st.openCh <- err
	})
}

func (st *muxStream) updateSendWindow(maxOff uint64) {
	st.sendMu.Lock()
	if maxOff > st.sendWindow {
		st.sendWindow = maxOff
		select {
		case st.winWake <- struct{}{}:
		default:
		}
	}
	st.sendMu.Unlock()
}

func (st *muxStream) onData(offset uint64, payload []byte) bool {
	if st.closed.Load() {
		return false
	}
	st.readMu.Lock()
	// Enforce window strictly relative to consumed offset
	if offset+uint64(len(payload)) > st.recvConsumed+uint64(st.sess.streamWindow) {
		st.readMu.Unlock()
		return false
	}
	chunks, ok := st.rb.Push(offset, payload)
	if !ok {
		st.readMu.Unlock()
		return false
	}
	for _, c := range chunks {
		st.ready = append(st.ready, c)
		st.readyBytes += len(c)
		st.sess.sessionBytes.Add(int64(len(c)))
	}
	if st.rb.IsEOF() {
		st.readEOF = true
	}
	eof := st.readEOF
	st.readMu.Unlock()
	if len(chunks) > 0 || eof {
		st.signalRead()
	}
	return true
}

func (st *muxStream) onFin(finalOffset uint64) {
	st.readMu.Lock()
	if st.rb.MarkFin(finalOffset) {
		st.readEOF = true
	}
	eof := st.readEOF
	st.readMu.Unlock()
	if eof {
		st.signalRead()
	}
}

func (st *muxStream) signalRead() {
	select {
	case st.readNotify <- struct{}{}:
	default:
	}
}

func (st *muxStream) Read(p []byte) (int, error) {
	for {
		st.readMu.Lock()
		if len(st.ready) > 0 {
			head := st.ready[0]
			n := copy(p, head)
			if n == len(head) {
				st.ready = st.ready[1:]
			} else {
				st.ready[0] = head[n:]
			}
			st.readyBytes -= n
			st.recvConsumed += uint64(n)
			st.sess.sessionBytes.Add(-int64(n))
			needWin := st.recvConsumed+uint64(st.sess.streamWindow) >= st.lastAdvertisedMax+uint64(st.sess.streamWindow)/4
			newMax := st.recvConsumed + uint64(st.sess.streamWindow)
			if needWin {
				st.lastAdvertisedMax = newMax
			}
			st.readMu.Unlock()
			if needWin {
				st.sendWindowUpdate(newMax)
			}
			return n, nil
		}
		if st.readEOF {
			st.readMu.Unlock()
			return 0, io.EOF
		}
		if err := st.getErr(); err != nil {
			st.readMu.Unlock()
			return 0, err
		}
		st.readMu.Unlock()

		select {
		case <-st.readNotify:
		case <-st.sess.ctx.Done():
			return 0, ErrSessionClosed
		}
	}
}

func (st *muxStream) maybeRefreshWindow() {
	if st.closed.Load() {
		return
	}
	st.readMu.Lock()
	eof := st.readEOF
	newMax := st.recvConsumed + uint64(st.sess.streamWindow)
	st.lastAdvertisedMax = newMax
	st.readMu.Unlock()
	if !eof {
		st.sendWindowUpdate(newMax)
	}
}

func (st *muxStream) sendWindowUpdate(maxOff uint64) {
	if p, ok := st.sess.sched.FastestPipe(); ok && p.Writer != nil {
		_ = p.Writer.TryEnqueueCtrl(Frame{
			Type:     FrameWindow,
			StreamID: st.id,
			Offset:   maxOff,
		})
	}
}

func (st *muxStream) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		if err := st.getErr(); err != nil {
			return written, err
		}
		st.sendMu.Lock()
		if st.writeEOF {
			st.sendMu.Unlock()
			return written, io.ErrClosedPipe
		}
		avail := int64(st.sendWindow) - int64(st.sendOffset)
		if avail <= 0 {
			st.sendMu.Unlock()
			t := time.NewTimer(200 * time.Millisecond)
			select {
			case <-st.winWake:
				t.Stop()
			case <-st.sess.ctx.Done():
				t.Stop()
				return written, ErrSessionClosed
			case <-t.C:
			}
			continue
		}
		n := len(p)
		if n > st.sess.chunkSize {
			n = st.sess.chunkSize
		}
		if int64(n) > avail {
			n = int(avail)
		}
		off := st.sendOffset
		st.sendOffset += uint64(n)
		st.sendMu.Unlock()

		chunk := append([]byte(nil), p[:n]...)
		if err := st.sess.sendReliable(st.sess.ctx, Frame{
			Type:     FrameData,
			StreamID: st.id,
			Offset:   off,
			Payload:  chunk,
		}); err != nil {
			return written, err
		}
		p = p[n:]
		written += n
	}
	return written, nil
}

func (st *muxStream) CloseWrite() error {
	st.sendMu.Lock()
	if st.writeEOF {
		st.sendMu.Unlock()
		return nil
	}
	st.writeEOF = true
	finOff := st.sendOffset
	st.sendMu.Unlock()
	if st.closed.Load() {
		return nil
	}
	return st.sess.sendReliable(st.sess.ctx, Frame{
		Type:     FrameFin,
		StreamID: st.id,
		Offset:   finOff,
	})
}

func (st *muxStream) Reset() {
	st.sess.resetStream(st.id)
}

func (st *muxStream) Close() error {
	if !st.closed.CompareAndSwap(false, true) {
		return nil
	}
	st.readMu.Lock()
	if st.readyBytes > 0 {
		st.sess.sessionBytes.Add(-int64(st.readyBytes))
		st.readyBytes = 0
	}
	st.ready = nil
	st.rb.Close()
	st.readMu.Unlock()
	st.sess.removeStream(st.id)
	st.signalRead()
	return nil
}

func (st *muxStream) abort(err error) {
	st.errMu.Lock()
	if st.err == nil {
		st.err = err
	}
	st.errMu.Unlock()
	st.resolveOpen(0, err)
	_ = st.Close()
	select {
	case st.winWake <- struct{}{}:
	default:
	}
}

func (st *muxStream) getErr() error {
	st.errMu.Lock()
	defer st.errMu.Unlock()
	return st.err
}

func bridgeStreamAndTCP(stream *muxStream, conn net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		_, err := io.CopyBuffer(stream, conn, buf)
		if err != nil {
			stream.Reset()
			_ = conn.Close()
			return
		}
		_ = stream.CloseWrite()
	}()
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		_, err := io.CopyBuffer(conn, stream, buf)
		if err != nil {
			stream.Reset()
			_ = conn.Close()
			return
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		} else {
			_ = conn.Close()
		}
	}()
	wg.Wait()
	_ = conn.Close()
	_ = stream.Close()
}

func formatSessionID(id [16]byte) string {
	return fmt.Sprintf("%x", id[:4])
}
