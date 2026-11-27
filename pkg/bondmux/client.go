package bondmux

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClientConfig struct {
	ListenAddr    string
	SocksUser     string
	SocksPass     string
	TargetAddr    string
	UUID          string
	RoomPorts     []int
	RoomUser      string
	RoomPass      string
	ChunkSize     int
	StreamWindow  uint32
	SessionWindow uint32
	MaxStreams    int
	ReadyTimeout  time.Duration
	Logf          func(string, ...any)
}

type udpFlow struct {
	conn       *net.UDPConn
	clientAddr atomic.Value // *net.UDPAddr
}

type Client struct {
	cfg       ClientConfig
	uuidBytes [16]byte
	sessionID [16]byte
	ln        net.Listener
	sess      *Session
	ctx       context.Context
	cancel    context.CancelFunc
	running   atomic.Bool

	pipeSeq     atomic.Uint32
	clockOffset atomic.Int64

	udpMu     sync.RWMutex
	udpFlows  map[uint32]*udpFlow
	nextFlow  atomic.Uint32

	diagMu sync.Mutex
	diag   []string

	wg sync.WaitGroup
}

func StartClient(parent context.Context, cfg ClientConfig) (*Client, error) {
	if cfg.TargetAddr == "" {
		return nil, errors.New("bondmux: target address is required")
	}
	if len(cfg.RoomPorts) == 0 {
		return nil, errors.New("bondmux: at least one room port is required")
	}
	uuidBytes, err := ParseUUIDBytes(cfg.UUID)
	if err != nil {
		return nil, err
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0"
	}
	if cfg.ReadyTimeout <= 0 {
		cfg.ReadyTimeout = 10 * time.Second
	}

	var sid [16]byte
	if _, err := io.ReadFull(rand.Reader, sid[:]); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(parent)
	c := &Client{
		cfg:       cfg,
		uuidBytes: uuidBytes,
		sessionID: sid,
		ctx:       ctx,
		cancel:    cancel,
		udpFlows:  make(map[uint32]*udpFlow),
	}
	c.sess = newSession(ctx, sid, false, cfg.ChunkSize, cfg.StreamWindow, cfg.SessionWindow, cfg.MaxStreams, c.logf)
	c.sess.onUDP = c.handleServerUDP
	c.running.Store(true)

	readyCh := make(chan struct{})
	var readyOnce sync.Once
	fatalCh := make(chan error, len(cfg.RoomPorts))
	var failedInitial atomic.Int32
	var lastHandshakeErr atomic.Value

	for _, port := range cfg.RoomPorts {
		roomPort := port
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.roomLoop(roomPort, &readyOnce, readyCh, fatalCh, &failedInitial, &lastHandshakeErr)
		}()
	}

	readyTimer := time.NewTimer(cfg.ReadyTimeout)
	defer readyTimer.Stop()

	select {
	case <-readyCh:
	case err := <-fatalCh:
		_ = c.Close()
		return nil, err
	case <-parent.Done():
		_ = c.Close()
		return nil, parent.Err()
	case <-readyTimer.C:
		_ = c.Close()
		if v := lastHandshakeErr.Load(); v != nil {
			return nil, v.(error)
		}
		return nil, fmt.Errorf("bondmux handshake: timed out waiting for first pipe (%v)", cfg.ReadyTimeout)
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("bondmux: listen %s: %w", cfg.ListenAddr, err)
	}
	c.ln = ln

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.serveSocks()
	}()
	c.logf("bondmux client ready on %s (session=%s rooms=%d)", ln.Addr().String(), formatSessionID(sid), len(cfg.RoomPorts))
	return c, nil
}

func (c *Client) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	c.diagMu.Lock()
	if len(c.diag) >= 64 {
		c.diag = c.diag[1:]
	}
	c.diag = append(c.diag, line)
	c.diagMu.Unlock()
	if c.cfg.Logf != nil {
		c.cfg.Logf("%s", line)
	}
}

func (c *Client) Diagnostics() string {
	c.diagMu.Lock()
	defer c.diagMu.Unlock()
	return strings.Join(c.diag, "\n")
}

func (c *Client) ListenAddr() string {
	return c.ln.Addr().String()
}

func (c *Client) Credentials() (string, string) {
	return c.cfg.SocksUser, c.cfg.SocksPass
}

func (c *Client) ActivePipes() int {
	return c.sess.ActivePipes()
}

func (c *Client) IsRunning() bool {
	return c.running.Load()
}

func (c *Client) Close() error {
	if !c.running.CompareAndSwap(true, false) {
		return nil
	}
	c.cancel()
	if c.ln != nil {
		_ = c.ln.Close()
	}
	c.sess.Close()
	c.udpMu.Lock()
	for id, f := range c.udpFlows {
		_ = f.conn.Close()
		delete(c.udpFlows, id)
	}
	c.udpMu.Unlock()
	c.wg.Wait()
	return nil
}

func (c *Client) roomLoop(
	roomPort int,
	readyOnce *sync.Once,
	readyCh chan struct{},
	fatalCh chan error,
	failedInitial *atomic.Int32,
	lastHandshakeErr *atomic.Value,
) {
	backoff := 200 * time.Millisecond
	firstAttemptDone := false
	recordInitialHandshakeFailure := func(err error) {
		lastHandshakeErr.Store(err)
		if errors.Is(err, ErrAuthFailed) {
			select {
			case fatalCh <- err:
			default:
			}
			return
		}
		if !firstAttemptDone {
			firstAttemptDone = true
			if int(failedInitial.Add(1)) >= len(c.cfg.RoomPorts) {
				select {
				case fatalCh <- err:
				default:
				}
			}
		}
	}

	for c.ctx.Err() == nil {
		conn, pipeID, err := c.dialRoomPipe(roomPort, false)
		if errors.Is(err, ErrClockSkew) {
			// Single automatic retry with corrected server clock offset
			conn, pipeID, err = c.dialRoomPipe(roomPort, true)
		}
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			if errors.Is(err, ErrAuthFailed) || errors.Is(err, ErrBadHandshake) || errors.Is(err, ErrClockSkew) {
				c.logf("bondmux room :%d %v", roomPort, err)
				recordInitialHandshakeFailure(err)
			}
			t := time.NewTimer(backoff)
			select {
			case <-c.ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			backoff = min(backoff*2, 3*time.Second)
			continue
		}

		backoff = 200 * time.Millisecond
		firstAttemptDone = true
		readyOnce.Do(func() { close(readyCh) })
		c.sess.AttachPipe(pipeID, conn, nil)
	}
}

func (c *Client) dialRoomPipe(roomPort int, isRetry bool) (net.Conn, uint16, error) {
	roomAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(roomPort))
	var d net.Dialer
	d.Timeout = 4 * time.Second
	conn, err := d.DialContext(c.ctx, "tcp", roomAddr)
	if err != nil {
		return nil, 0, err
	}
	stopCancel := context.AfterFunc(c.ctx, func() { _ = conn.Close() })
	defer stopCancel()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	_ = conn.SetDeadline(time.Now().Add(HandshakeTimeout))
	if err := dialViaSocks5(conn, c.cfg.RoomUser, c.cfg.RoomPass, c.cfg.TargetAddr); err != nil {
		_ = conn.Close()
		return nil, 0, err
	}
	pipeID := uint16(c.pipeSeq.Add(1))
	if err := ClientHandshake(conn, c.uuidBytes, c.sessionID, pipeID, &c.clockOffset); err != nil {
		_ = conn.Close()
		if isRetry {
			return nil, 0, fmt.Errorf("%w (after clock retry)", err)
		}
		return nil, 0, err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, pipeID, nil
}

func dialViaSocks5(conn net.Conn, user, pass, target string) error {
	if user != "" || pass != "" {
		if _, err := conn.Write([]byte{0x05, 0x02, 0x00, 0x02}); err != nil {
			return err
		}
	} else {
		if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
			return err
		}
	}
	var mrep [2]byte
	if _, err := io.ReadFull(conn, mrep[:]); err != nil {
		return err
	}
	if mrep[0] != 0x05 {
		return errors.New("socks5: invalid version")
	}
	switch mrep[1] {
	case 0x00:
	case 0x02:
		if len(user) > 255 || len(pass) > 255 {
			return errors.New("socks5: credentials too long")
		}
		auth := make([]byte, 0, 3+len(user)+len(pass))
		auth = append(auth, 0x01, byte(len(user)))
		auth = append(auth, user...)
		auth = append(auth, byte(len(pass)))
		auth = append(auth, pass...)
		if _, err := conn.Write(auth); err != nil {
			return err
		}
		var arep [2]byte
		if _, err := io.ReadFull(conn, arep[:]); err != nil {
			return err
		}
		if arep[1] != 0x00 {
			return errors.New("socks5: room auth failed")
		}
	default:
		return fmt.Errorf("socks5: unsupported auth method 0x%02x", mrep[1])
	}

	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	portNum, err := strconv.Atoi(portStr)
	if err != nil || portNum <= 0 || portNum > 65535 {
		return ErrInvalidAddr
	}
	dst := TargetAddr{Host: host, Port: uint16(portNum)}
	if ip := net.ParseIP(host); ip != nil {
		if ip.To4() != nil {
			dst.Atyp = AtypIPv4
		} else {
			dst.Atyp = AtypIPv6
		}
	} else {
		dst.Atyp = AtypDomain
	}
	ab, err := EncodeTargetAddr(dst)
	if err != nil {
		return err
	}
	req := append([]byte{0x05, 0x01, 0x00}, ab...)
	if _, err := conn.Write(req); err != nil {
		return err
	}
	var rep [4]byte
	if _, err := io.ReadFull(conn, rep[:]); err != nil {
		return err
	}
	if rep[0] != 0x05 || rep[1] != 0x00 {
		return fmt.Errorf("socks5: connect refused (0x%02x)", rep[1])
	}
	switch rep[3] {
	case AtypIPv4:
		_, err = io.ReadFull(conn, make([]byte, 6))
	case AtypIPv6:
		_, err = io.ReadFull(conn, make([]byte, 18))
	case AtypDomain:
		var l [1]byte
		if _, err = io.ReadFull(conn, l[:]); err == nil {
			_, err = io.ReadFull(conn, make([]byte, int(l[0])+2))
		}
	default:
		return ErrInvalidAddr
	}
	return err
}

func (c *Client) serveSocks() {
	for {
		conn, err := c.ln.Accept()
		if err != nil {
			return
		}
		go c.handleSocksConn(conn)
	}
}

func (c *Client) handleSocksConn(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if err := c.socksAuth(conn); err != nil {
		_ = conn.Close()
		return
	}
	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil || hdr[0] != 0x05 {
		_ = conn.Close()
		return
	}
	dst, err := readSocksTarget(conn, hdr[3])
	if err != nil {
		_ = conn.Close()
		return
	}
	_ = conn.SetDeadline(time.Time{})

	switch hdr[1] {
	case 0x01: // CONNECT
		stream, err := c.sess.OpenTCP(c.ctx, dst)
		if err != nil {
			_, _ = conn.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
			_ = conn.Close()
			return
		}
		if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
			stream.Reset()
			_ = conn.Close()
			return
		}
		bridgeStreamAndTCP(stream, conn)
	case 0x03: // UDP ASSOCIATE
		c.handleSocksUDP(conn)
	default:
		_, _ = conn.Write([]byte{0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		_ = conn.Close()
	}
}

func (c *Client) socksAuth(conn net.Conn) error {
	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil || hdr[0] != 0x05 {
		return errors.New("bad socks version")
	}
	methods := make([]byte, hdr[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	requireAuth := c.cfg.SocksUser != "" || c.cfg.SocksPass != ""
	if !requireAuth {
		_, err := conn.Write([]byte{0x05, 0x00})
		return err
	}
	supportsPass := false
	for _, m := range methods {
		if m == 0x02 {
			supportsPass = true
		}
	}
	if !supportsPass {
		_, _ = conn.Write([]byte{0x05, 0xFF})
		return errors.New("no acceptable auth method")
	}
	if _, err := conn.Write([]byte{0x05, 0x02}); err != nil {
		return err
	}
	var ah [2]byte
	if _, err := io.ReadFull(conn, ah[:]); err != nil || ah[0] != 0x01 {
		return errors.New("bad auth version")
	}
	ub := make([]byte, ah[1])
	if _, err := io.ReadFull(conn, ub); err != nil {
		return err
	}
	var pl [1]byte
	if _, err := io.ReadFull(conn, pl[:]); err != nil {
		return err
	}
	pb := make([]byte, pl[0])
	if _, err := io.ReadFull(conn, pb); err != nil {
		return err
	}
	if string(ub) != c.cfg.SocksUser || string(pb) != c.cfg.SocksPass {
		_, _ = conn.Write([]byte{0x01, 0x01})
		return errors.New("invalid credentials")
	}
	_, err := conn.Write([]byte{0x01, 0x00})
	return err
}

func readSocksTarget(r io.Reader, atyp byte) (TargetAddr, error) {
	dst := TargetAddr{Atyp: atyp}
	switch atyp {
	case AtypIPv4:
		var b [4]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return TargetAddr{}, err
		}
		dst.Host = net.IP(b[:]).String()
	case AtypIPv6:
		var b [16]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return TargetAddr{}, err
		}
		dst.Host = net.IP(b[:]).String()
	case AtypDomain:
		var l [1]byte
		if _, err := io.ReadFull(r, l[:]); err != nil {
			return TargetAddr{}, err
		}
		b := make([]byte, l[0])
		if _, err := io.ReadFull(r, b); err != nil {
			return TargetAddr{}, err
		}
		dst.Host = string(b)
	default:
		return TargetAddr{}, ErrInvalidAddr
	}
	var pb [2]byte
	if _, err := io.ReadFull(r, pb[:]); err != nil {
		return TargetAddr{}, err
	}
	dst.Port = binary.BigEndian.Uint16(pb[:])
	return dst, nil
}

func (c *Client) handleSocksUDP(ctrl net.Conn) {
	defer ctrl.Close()
	udpLn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		_, _ = ctrl.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer udpLn.Close()
	port := uint16(udpLn.LocalAddr().(*net.UDPAddr).Port)
	rep := []byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, byte(port >> 8), byte(port)}
	if _, err := ctrl.Write(rep); err != nil {
		return
	}

	flowID := c.nextFlow.Add(1)
	fl := &udpFlow{conn: udpLn}
	c.udpMu.Lock()
	c.udpFlows[flowID] = fl
	c.udpMu.Unlock()
	defer func() {
		c.udpMu.Lock()
		delete(c.udpFlows, flowID)
		c.udpMu.Unlock()
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, MaxPayload+64)
		for {
			n, addr, err := udpLn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < 10 || buf[2] != 0x00 {
				continue
			}
			fl.clientAddr.Store(addr)
			dst, data, err := DecodeUDPPkt(buf[3:n])
			if err != nil {
				continue
			}
			_ = c.sess.SendUDP(flowID, dst, data)
		}
	}()

	// Hold UDP associate open until TCP control connection closes
	_, _ = io.Copy(io.Discard, ctrl)
	_ = udpLn.Close()
	<-done
}

func (c *Client) handleServerUDP(flowID uint32, src TargetAddr, data []byte) {
	c.udpMu.RLock()
	fl := c.udpFlows[flowID]
	c.udpMu.RUnlock()
	if fl == nil {
		return
	}
	v := fl.clientAddr.Load()
	if v == nil {
		return
	}
	clientAddr := v.(*net.UDPAddr)
	ab, err := EncodeTargetAddr(src)
	if err != nil {
		return
	}
	pkt := make([]byte, 3+len(ab)+len(data))
	copy(pkt[3:], ab)
	copy(pkt[3+len(ab):], data)
	_, _ = fl.conn.WriteToUDP(pkt, clientAddr)
}
