package bondmux

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultSessionGrace = 60 * time.Second
	defaultUDPIdle      = 60 * time.Second
)

type ServerConfig struct {
	ListenAddr           string
	FallbackAddr         string
	UUIDs                []string
	DNS                  string
	ChunkSize            int
	StreamWindow         uint32
	SessionWindow        uint32
	MaxStreams           int
	SessionGrace         time.Duration
	AllowLoopbackForTest bool
	LookupIP             func(ctx context.Context, host string) ([]netip.Addr, error)
	DialTCPForTest       func(ctx context.Context, addr netip.Addr, port uint16) (net.Conn, error)
	Logf                 func(string, ...any)
}

type serverSession struct {
	sess       *Session
	graceMu    sync.Mutex
	graceTimer *time.Timer
	udpMu      sync.Mutex
	udpSockets map[uint32]*serverUDPSock
}

type serverUDPSock struct {
	conn     *net.UDPConn
	lastUsed atomic.Int64
}

type Server struct {
	cfg      ServerConfig
	uuids    [][16]byte
	egress   *egressDialer
	ln       net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	closed   atomic.Bool
	logf     func(string, ...any)

	mu       sync.Mutex
	sessions map[[16]byte]*serverSession
}

func StartServer(parent context.Context, cfg ServerConfig) (*Server, error) {
	if len(cfg.UUIDs) == 0 {
		return nil, errors.New("bondmux: at least one UUID is required")
	}
	uuids := make([][16]byte, 0, len(cfg.UUIDs))
	for _, raw := range cfg.UUIDs {
		u, err := ParseUUIDBytes(raw)
		if err != nil {
			return nil, err
		}
		uuids = append(uuids, u)
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:10000"
	}
	if cfg.SessionGrace <= 0 {
		cfg.SessionGrace = defaultSessionGrace
	}
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("bondmux server listen %s: %w", cfg.ListenAddr, err)
	}
	ctx, cancel := context.WithCancel(parent)
	s := &Server{
		cfg:      cfg,
		uuids:    uuids,
		egress:   newEgressDialer(cfg.DNS, cfg.AllowLoopbackForTest, cfg.LookupIP, cfg.DialTCPForTest),
		ln:       ln,
		ctx:      ctx,
		cancel:   cancel,
		logf:     logf,
		sessions: make(map[[16]byte]*serverSession),
	}
	go s.acceptLoop()
	return s, nil
}

func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.cancel()
	err := s.ln.Close()
	s.mu.Lock()
	all := make([]*serverSession, 0, len(s.sessions))
	for _, ss := range s.sessions {
		all = append(all, ss)
	}
	s.sessions = make(map[[16]byte]*serverSession)
	s.mu.Unlock()
	for _, ss := range all {
		ss.closeAll()
	}
	return err
}

func (ss *serverSession) closeAll() {
	ss.graceMu.Lock()
	if ss.graceTimer != nil {
		ss.graceTimer.Stop()
		ss.graceTimer = nil
	}
	ss.graceMu.Unlock()
	ss.udpMu.Lock()
	for id, u := range ss.udpSockets {
		_ = u.conn.Close()
		delete(ss.udpSockets, id)
	}
	ss.udpMu.Unlock()
	ss.sess.Close()
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	br := bufio.NewReaderSize(conn, 4096)
	_ = conn.SetReadDeadline(time.Now().Add(HandshakeTimeout))
	sid, pipeID, isMux, prefix, err := ServerHandshake(br, conn, s.uuids, time.Now)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		_ = conn.Close()
		return
	}
	if !isMux {
		s.spliceFallback(conn, br, prefix)
		return
	}
	ss := s.getOrCreateSession(sid)
	ss.graceMu.Lock()
	if ss.graceTimer != nil {
		ss.graceTimer.Stop()
		ss.graceTimer = nil
	}
	ss.graceMu.Unlock()

	// Note: ServerHandshake already drained exact 62 bytes via br; any unread buffered bytes
	// after handshake must be fed through brConn so frames aren't lost.
	pipeConn := &bufferedConn{Conn: conn, br: br}
	ss.sess.AttachPipe(pipeID, pipeConn, func() {
		if ss.sess.ActivePipes() == 0 {
			s.scheduleGrace(sid, ss)
		}
	})
}

func (s *Server) spliceFallback(client net.Conn, br *bufio.Reader, prefix []byte) {
	defer client.Close()
	if s.cfg.FallbackAddr == "" {
		return
	}
	fb, err := net.DialTimeout("tcp", s.cfg.FallbackAddr, 5*time.Second)
	if err != nil {
		return
	}
	defer fb.Close()
	if tc, ok := fb.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if len(prefix) > 0 {
		if _, err := fb.Write(prefix); err != nil {
			return
		}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(fb, br)
		if tc, ok := fb.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, fb)
		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	wg.Wait()
}

func (s *Server) getOrCreateSession(sid [16]byte) *serverSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ss, ok := s.sessions[sid]; ok {
		return ss
	}
	sess := newSession(s.ctx, sid, true, s.cfg.ChunkSize, s.cfg.StreamWindow, s.cfg.SessionWindow, s.cfg.MaxStreams, s.logf)
	ss := &serverSession{
		sess:       sess,
		udpSockets: make(map[uint32]*serverUDPSock),
	}
	sess.onOpenTCP = func(st *muxStream, dst TargetAddr) {
		s.handleOpenTCP(ss, st, dst)
	}
	sess.onUDP = func(flowID uint32, dst TargetAddr, data []byte) {
		s.handleUDP(ss, flowID, dst, data)
	}
	s.sessions[sid] = ss
	return ss
}

func (s *Server) scheduleGrace(sid [16]byte, ss *serverSession) {
	ss.graceMu.Lock()
	defer ss.graceMu.Unlock()
	if ss.graceTimer != nil {
		ss.graceTimer.Stop()
	}
	ss.graceTimer = time.AfterFunc(s.cfg.SessionGrace, func() {
		if ss.sess.ActivePipes() > 0 {
			return
		}
		s.mu.Lock()
		if cur, ok := s.sessions[sid]; ok && cur == ss {
			delete(s.sessions, sid)
		}
		s.mu.Unlock()
		ss.closeAll()
	})
}

func (s *Server) handleOpenTCP(ss *serverSession, st *muxStream, dst TargetAddr) {
	dialCtx, cancel := context.WithTimeout(ss.sess.ctx, 10*time.Second)
	conn, err := s.egress.dialTCP(dialCtx, dst)
	cancel()
	if err != nil {
		_ = ss.sess.sendReliableAsync(Frame{
			Type:     FrameOpenOK,
			Flags:    FlagOpenFail,
			StreamID: st.id,
			Payload:  EncodeOpenOK(0, err.Error()),
		})
		_ = st.Close()
		return
	}
	if err := ss.sess.sendReliableAsync(Frame{
		Type:     FrameOpenOK,
		StreamID: st.id,
		Payload:  EncodeOpenOK(ss.sess.streamWindow, ""),
	}); err != nil {
		_ = conn.Close()
		_ = st.Close()
		return
	}
	bridgeStreamAndTCP(st, conn)
}

func (s *Server) handleUDP(ss *serverSession, flowID uint32, dst TargetAddr, data []byte) {
	dialCtx, cancel := context.WithTimeout(ss.sess.ctx, 4*time.Second)
	udpAddr, err := s.egress.resolveUDP(dialCtx, dst)
	cancel()
	if err != nil {
		return
	}

	ss.udpMu.Lock()
	sock := ss.udpSockets[flowID]
	if sock == nil {
		c, err := net.ListenUDP("udp", nil)
		if err != nil {
			ss.udpMu.Unlock()
			return
		}
		sock = &serverUDPSock{conn: c}
		sock.lastUsed.Store(time.Now().UnixNano())
		ss.udpSockets[flowID] = sock
		go s.readServerUDP(ss, flowID, sock)
	} else {
		sock.lastUsed.Store(time.Now().UnixNano())
	}
	ss.udpMu.Unlock()

	_, _ = sock.conn.WriteToUDP(data, udpAddr)
}

func (s *Server) readServerUDP(ss *serverSession, flowID uint32, sock *serverUDPSock) {
	defer func() {
		_ = sock.conn.Close()
		ss.udpMu.Lock()
		if cur, ok := ss.udpSockets[flowID]; ok && cur == sock {
			delete(ss.udpSockets, flowID)
		}
		ss.udpMu.Unlock()
	}()
	buf := make([]byte, MaxPayload)
	for {
		_ = sock.conn.SetReadDeadline(time.Now().Add(defaultUDPIdle))
		n, addr, err := sock.conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if time.Since(time.Unix(0, sock.lastUsed.Load())) >= defaultUDPIdle {
					return
				}
				continue
			}
			return
		}
		sock.lastUsed.Store(time.Now().UnixNano())
		src := TargetAddr{Host: addr.IP.String(), Port: uint16(addr.Port)}
		if ip4 := addr.IP.To4(); ip4 != nil {
			src.Atyp = AtypIPv4
			src.Host = ip4.String()
		} else {
			src.Atyp = AtypIPv6
		}
		_ = ss.sess.SendUDP(flowID, src, buf[:n])
	}
}

type bufferedConn struct {
	net.Conn
	br *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) {
	return b.br.Read(p)
}
