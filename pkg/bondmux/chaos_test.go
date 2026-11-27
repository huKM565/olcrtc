package bondmux

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testUUID = "2b9618c0-76da-4f8d-bf1f-3270d30ff6a3"

type mockSocksRoom struct {
	ln           net.Listener
	port         int
	bytesPerSec  int64
	stallAfterHS atomic.Bool
	mu           sync.Mutex
	conns        []net.Conn
	closed       bool
}

func startMockSocksRoom(t *testing.T, bytesPerSec int64) *mockSocksRoom {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock room: %v", err)
	}
	m := &mockSocksRoom{
		ln:          ln,
		port:        ln.Addr().(*net.TCPAddr).Port,
		bytesPerSec: bytesPerSec,
	}
	go m.serve()
	t.Cleanup(func() { m.Close() })
	return m
}

func (m *mockSocksRoom) DropActive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.conns {
		_ = c.Close()
	}
	m.conns = nil
}

func (m *mockSocksRoom) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	conns := m.conns
	m.conns = nil
	m.mu.Unlock()
	_ = m.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (m *mockSocksRoom) serve() {
	for {
		c, err := m.ln.Accept()
		if err != nil {
			return
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			_ = c.Close()
			return
		}
		m.conns = append(m.conns, c)
		m.mu.Unlock()
		go m.handle(c)
	}
}

func (m *mockSocksRoom) handle(c net.Conn) {
	defer c.Close()
	var hdr [2]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil || hdr[0] != 0x05 {
		return
	}
	methods := make([]byte, hdr[1])
	if _, err := io.ReadFull(c, methods); err != nil {
		return
	}
	hasUserPass := false
	for _, mm := range methods {
		if mm == 0x02 {
			hasUserPass = true
		}
	}
	if hasUserPass {
		_, _ = c.Write([]byte{0x05, 0x02})
		var ah [2]byte
		if _, err := io.ReadFull(c, ah[:]); err != nil {
			return
		}
		u := make([]byte, ah[1])
		if _, err := io.ReadFull(c, u); err != nil {
			return
		}
		var pl [1]byte
		if _, err := io.ReadFull(c, pl[:]); err != nil {
			return
		}
		p := make([]byte, pl[0])
		if _, err := io.ReadFull(c, p); err != nil {
			return
		}
		_, _ = c.Write([]byte{0x01, 0x00})
	} else {
		_, _ = c.Write([]byte{0x05, 0x00})
	}

	var req [4]byte
	if _, err := io.ReadFull(c, req[:]); err != nil || req[1] != 0x01 {
		return
	}
	var host string
	switch req[3] {
	case 0x01:
		var ip [4]byte
		if _, err := io.ReadFull(c, ip[:]); err != nil {
			return
		}
		host = net.IP(ip[:]).String()
	case 0x03:
		var l [1]byte
		if _, err := io.ReadFull(c, l[:]); err != nil {
			return
		}
		d := make([]byte, l[0])
		if _, err := io.ReadFull(c, d); err != nil {
			return
		}
		host = string(d)
	case 0x04:
		var ip [16]byte
		if _, err := io.ReadFull(c, ip[:]); err != nil {
			return
		}
		host = net.IP(ip[:]).String()
	default:
		return
	}
	var portBuf [2]byte
	if _, err := io.ReadFull(c, portBuf[:]); err != nil {
		return
	}
	port := binary.BigEndian.Uint16(portBuf[:])
	dst, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(int(port))), 3*time.Second)
	if err != nil {
		_, _ = c.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer dst.Close()
	_, _ = c.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0})

	if m.stallAfterHS.Load() {
		// Forward handshake (62B client -> server, 2B server -> client) then stall client->server
		var hs [HandshakeSize]byte
		if _, err := io.ReadFull(c, hs[:]); err != nil {
			return
		}
		if _, err := dst.Write(hs[:]); err != nil {
			return
		}
		var resp [2]byte
		if _, err := io.ReadFull(dst, resp[:]); err != nil {
			return
		}
		if _, err := c.Write(resp[:]); err != nil {
			return
		}
		time.Sleep(5 * time.Second)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		copyRateLimited(dst, c, m.bytesPerSec)
		_ = dst.Close()
	}()
	go func() {
		defer wg.Done()
		copyRateLimited(c, dst, m.bytesPerSec)
		_ = c.Close()
	}()
	wg.Wait()
}

func copyRateLimited(dst io.Writer, src io.Reader, bytesPerSec int64) {
	if bytesPerSec <= 0 {
		_, _ = io.Copy(dst, src)
		return
	}
	buf := make([]byte, 8192)
	start := time.Now()
	var total int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
			total += int64(n)
			want := time.Duration(float64(total) / float64(bytesPerSec) * float64(time.Second))
			if sleep := want - time.Since(start); sleep > 0 {
				time.Sleep(sleep)
			}
		}
		if err != nil {
			return
		}
	}
}

func socks5Connect(t *testing.T, proxyAddr, user, pass, host string, port uint16, atyp byte) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial socks5 %s: %v", proxyAddr, err)
	}
	if user != "" {
		_, _ = conn.Write([]byte{0x05, 0x01, 0x02})
		var rep [2]byte
		if _, err := io.ReadFull(conn, rep[:]); err != nil || rep[1] != 0x02 {
			conn.Close()
			t.Fatalf("socks5 auth method: %x err=%v", rep, err)
		}
		auth := append([]byte{0x01, byte(len(user))}, []byte(user)...)
		auth = append(auth, byte(len(pass)))
		auth = append(auth, []byte(pass)...)
		_, _ = conn.Write(auth)
		if _, err := io.ReadFull(conn, rep[:]); err != nil || rep[1] != 0x00 {
			conn.Close()
			t.Fatalf("socks5 auth reply: %x err=%v", rep, err)
		}
	} else {
		_, _ = conn.Write([]byte{0x05, 0x01, 0x00})
		var rep [2]byte
		if _, err := io.ReadFull(conn, rep[:]); err != nil || rep[1] != 0x00 {
			conn.Close()
			t.Fatalf("socks5 no-auth reply: %x err=%v", rep, err)
		}
	}

	addrBytes, err := EncodeTargetAddr(TargetAddr{Atyp: atyp, Host: host, Port: port})
	if err != nil {
		conn.Close()
		t.Fatalf("EncodeTargetAddr: %v", err)
	}
	req := append([]byte{0x05, 0x01, 0x00}, addrBytes...)
	if _, err := conn.Write(req); err != nil {
		conn.Close()
		t.Fatalf("write connect req: %v", err)
	}
	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		conn.Close()
		t.Fatalf("read connect reply: %v", err)
	}
	if hdr[1] != 0x00 {
		conn.Close()
		t.Fatalf("socks5 connect failed code=0x%02x", hdr[1])
	}
	switch hdr[3] {
	case 0x01:
		_, _ = io.ReadFull(conn, make([]byte, 6))
	case 0x03:
		var l [1]byte
		_, _ = io.ReadFull(conn, l[:])
		_, _ = io.ReadFull(conn, make([]byte, int(l[0])+2))
	case 0x04:
		_, _ = io.ReadFull(conn, make([]byte, 18))
	}
	return conn
}

func TestSingleStreamAggregatesBandwidthAcrossRooms(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Target TCP server streaming 4 MiB deterministic payload
	payload := make([]byte, 4<<20)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	wantHash := sha256.Sum256(payload)

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	go func() {
		for {
			c, err := targetLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = conn.Write(payload)
			}(c)
		}
	}()

	srv, err := StartServer(ctx, ServerConfig{
		ListenAddr:           "127.0.0.1:0",
		UUIDs:                []string{testUUID},
		AllowLoopbackForTest: true,
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	rooms := make([]*mockSocksRoom, 4)
	ports := make([]int, 4)
	for i := range rooms {
		rooms[i] = startMockSocksRoom(t, 1<<20) // 1 MiB/s per room
		ports[i] = rooms[i].port
	}

	cli, err := StartClient(ctx, ClientConfig{
		ListenAddr:   "127.0.0.1:0",
		SocksUser:    "u",
		SocksPass:    "p",
		TargetAddr:   srv.Addr(),
		UUID:         testUUID,
		RoomPorts:    ports,
		RoomUser:     "ru",
		RoomPass:     "rp",
		ReadyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartClient: %v", err)
	}
	defer cli.Close()

	// Wait briefly for all 4 rooms to attach
	deadline := time.Now().Add(2 * time.Second)
	for cli.ActivePipes() < 4 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if cli.ActivePipes() != 4 {
		t.Fatalf("expected 4 active pipes, got %d", cli.ActivePipes())
	}

	targetPort := uint16(targetLn.Addr().(*net.TCPAddr).Port)
	start := time.Now()
	conn := socks5Connect(t, cli.ListenAddr(), "u", "p", "127.0.0.1", targetPort, AtypIPv4)
	got, err := io.ReadAll(conn)
	conn.Close()
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if sha256.Sum256(got) != wantHash {
		t.Fatalf("payload hash mismatch: got %d bytes", len(got))
	}
	mbps := float64(len(got)) / (1 << 20) / elapsed.Seconds()
	if mbps < 2.0 {
		t.Fatalf("expected aggregated throughput >= 2.0 MiB/s across 4x1 MiB/s rooms, got %.2f MiB/s (%v)", mbps, elapsed)
	}
}

func TestHalfCloseAndLostOpenRetransmit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqData := bytes.Repeat([]byte("REQ!"), 32*1024) // 128 KiB
	respData := bytes.Repeat([]byte("RESP"), 64*1024) // 256 KiB

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	go func() {
		c, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		got, err := io.ReadAll(c)
		if err != nil || !bytes.Equal(got, reqData) {
			return
		}
		_, _ = c.Write(respData)
	}()

	srv, err := StartServer(ctx, ServerConfig{
		ListenAddr:           "127.0.0.1:0",
		UUIDs:                []string{testUUID},
		AllowLoopbackForTest: true,
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	r0 := startMockSocksRoom(t, 0)
	r0.stallAfterHS.Store(true)
	r1 := startMockSocksRoom(t, 0)

	cli, err := StartClient(ctx, ClientConfig{
		ListenAddr:   "127.0.0.1:0",
		TargetAddr:   srv.Addr(),
		UUID:         testUUID,
		RoomPorts:    []int{r0.port, r1.port},
		ReadyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartClient: %v", err)
	}
	defer cli.Close()

	targetPort := uint16(targetLn.Addr().(*net.TCPAddr).Port)
	conn := socks5Connect(t, cli.ListenAddr(), "", "", "127.0.0.1", targetPort, AtypIPv4)
	defer conn.Close()
	if _, err := conn.Write(reqData); err != nil {
		t.Fatalf("write reqData: %v", err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("ReadAll response: %v", err)
	}
	if !bytes.Equal(got, respData) {
		t.Fatalf("response mismatch: got %d bytes want %d", len(got), len(respData))
	}
}

func TestRoomDropAndRejoinMidTransfer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := make([]byte, 4<<20)
	for i := range payload {
		payload[i] = byte(i*17 + 3)
	}
	wantHash := sha256.Sum256(payload)

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	go func() {
		c, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write(payload)
	}()

	srv, err := StartServer(ctx, ServerConfig{
		ListenAddr:           "127.0.0.1:0",
		UUIDs:                []string{testUUID},
		AllowLoopbackForTest: true,
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	rooms := make([]*mockSocksRoom, 4)
	ports := make([]int, 4)
	for i := range rooms {
		rooms[i] = startMockSocksRoom(t, 1<<20)
		ports[i] = rooms[i].port
	}

	cli, err := StartClient(ctx, ClientConfig{
		ListenAddr:   "127.0.0.1:0",
		TargetAddr:   srv.Addr(),
		UUID:         testUUID,
		RoomPorts:    ports,
		ReadyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartClient: %v", err)
	}
	defer cli.Close()

	targetPort := uint16(targetLn.Addr().(*net.TCPAddr).Port)
	conn := socks5Connect(t, cli.ListenAddr(), "", "", "127.0.0.1", targetPort, AtypIPv4)
	defer conn.Close()

	go func() {
		time.Sleep(250 * time.Millisecond)
		rooms[0].Close()      // permanently killed
		rooms[1].DropActive() // drops active pipe and auto-rejoins
	}()

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("ReadAll during room drop: %v", err)
	}
	if sha256.Sum256(got) != wantHash {
		t.Fatalf("hash mismatch after room drop: got %d bytes", len(got))
	}
}

func TestWindowBackpressureAndRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := make([]byte, 3<<20)
	for i := range payload {
		payload[i] = byte(i)
	}
	wantHash := sha256.Sum256(payload)

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	go func() {
		c, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write(payload)
	}()

	srv, err := StartServer(ctx, ServerConfig{
		ListenAddr:           "127.0.0.1:0",
		UUIDs:                []string{testUUID},
		AllowLoopbackForTest: true,
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	r0 := startMockSocksRoom(t, 0)
	r1 := startMockSocksRoom(t, 0)

	cli, err := StartClient(ctx, ClientConfig{
		ListenAddr:    "127.0.0.1:0",
		TargetAddr:    srv.Addr(),
		UUID:          testUUID,
		RoomPorts:     []int{r0.port, r1.port},
		StreamWindow:  DefaultStreamWindowMobile,
		SessionWindow: DefaultSessionWindowMobile,
		ReadyTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartClient: %v", err)
	}
	defer cli.Close()

	targetPort := uint16(targetLn.Addr().(*net.TCPAddr).Port)
	conn := socks5Connect(t, cli.ListenAddr(), "", "", "127.0.0.1", targetPort, AtypIPv4)
	defer conn.Close()

	// Pause reading to force 1 MiB window exhaustion + 250ms WINDOW refresh
	time.Sleep(600 * time.Millisecond)

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("ReadAll after pause: %v", err)
	}
	if sha256.Sum256(got) != wantHash {
		t.Fatalf("hash mismatch: got %d bytes", len(got))
	}
}

func TestUDPAssociateAndVlessFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fallback echo TCP server (simulates Xray on 127.0.0.1:10002)
	fbLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fallback: %v", err)
	}
	defer fbLn.Close()
	go func() {
		for {
			c, err := fbLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	// UDP echo server
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer udpConn.Close()
	go func() {
		buf := make([]byte, 2048)
		for {
			n, addr, err := udpConn.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = udpConn.WriteTo(buf[:n], addr)
		}
	}()

	srv, err := StartServer(ctx, ServerConfig{
		ListenAddr:           "127.0.0.1:0",
		FallbackAddr:         fbLn.Addr().String(),
		UUIDs:                []string{testUUID},
		AllowLoopbackForTest: true,
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	// 1. VLESS fallback test (prefix starts with 0x00)
	raw, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial srv fallback: %v", err)
	}
	vlessMsg := []byte{0x00, 'v', 'l', 'e', 's', 's', '!'}
	_, _ = raw.Write(vlessMsg)
	echoed := make([]byte, len(vlessMsg))
	if _, err := io.ReadFull(raw, echoed); err != nil {
		raw.Close()
		t.Fatalf("read fallback echo: %v", err)
	}
	raw.Close()
	if !bytes.Equal(echoed, vlessMsg) {
		t.Fatalf("fallback echo mismatch: got %x want %x", echoed, vlessMsg)
	}

	// 2. UDP ASSOCIATE test
	r0 := startMockSocksRoom(t, 0)
	cli, err := StartClient(ctx, ClientConfig{
		ListenAddr:   "127.0.0.1:0",
		TargetAddr:   srv.Addr(),
		UUID:         testUUID,
		RoomPorts:    []int{r0.port},
		ReadyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartClient: %v", err)
	}
	defer cli.Close()

	ctrl, err := net.DialTimeout("tcp", cli.ListenAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial socks5 udp ctrl: %v", err)
	}
	defer ctrl.Close()
	_, _ = ctrl.Write([]byte{0x05, 0x01, 0x00})
	var mrep [2]byte
	if _, err := io.ReadFull(ctrl, mrep[:]); err != nil || mrep[1] != 0x00 {
		t.Fatalf("udp auth: %x err=%v", mrep, err)
	}
	_, _ = ctrl.Write([]byte{0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	var urep [10]byte
	if _, err := io.ReadFull(ctrl, urep[:]); err != nil || urep[1] != 0x00 {
		t.Fatalf("udp associate rep: %x err=%v", urep, err)
	}
	relayPort := binary.BigEndian.Uint16(urep[8:10])
	relayAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(relayPort)}

	clientUDP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("listen client udp: %v", err)
	}
	defer clientUDP.Close()

	udpDstPort := uint16(udpConn.LocalAddr().(*net.UDPAddr).Port)
	addrBytes, _ := EncodeTargetAddr(TargetAddr{Atyp: AtypIPv4, Host: "127.0.0.1", Port: udpDstPort})
	pkt := append([]byte{0, 0, 0}, addrBytes...)
	pkt = append(pkt, []byte("ping-udp")...)
	if _, err := clientUDP.WriteToUDP(pkt, relayAddr); err != nil {
		t.Fatalf("write udp: %v", err)
	}
	_ = clientUDP.SetReadDeadline(time.Now().Add(3 * time.Second))
	rbuf := make([]byte, 2048)
	n, _, err := clientUDP.ReadFromUDP(rbuf)
	if err != nil {
		t.Fatalf("read udp reply: %v", err)
	}
	_, body, err := DecodeUDPPkt(rbuf[3:n])
	if err != nil || string(body) != "ping-udp" {
		t.Fatalf("unexpected udp reply body=%q err=%v", string(body), err)
	}
}

func TestDomainPassthroughAndBlockedRanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var resolvedHost atomic.Value
	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	go func() {
		c, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write([]byte("domain-ok"))
	}()

	srv, err := StartServer(ctx, ServerConfig{
		ListenAddr: "127.0.0.1:0",
		UUIDs:      []string{testUUID},
		LookupIP: func(_ context.Context, host string) ([]netip.Addr, error) {
			resolvedHost.Store(host)
			if host == "loop.test.invalid" {
				return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
			}
			if host == "public.test.invalid" {
				return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
			}
			return nil, errors.New("nxdomain")
		},
		DialTCPForTest: func(ctx context.Context, addr netip.Addr, port uint16) (net.Conn, error) {
			return net.DialTimeout("tcp", targetLn.Addr().String(), 2*time.Second)
		},
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	r0 := startMockSocksRoom(t, 0)
	cli, err := StartClient(ctx, ClientConfig{
		ListenAddr:   "127.0.0.1:0",
		TargetAddr:   srv.Addr(),
		UUID:         testUUID,
		RoomPorts:    []int{r0.port},
		ReadyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartClient: %v", err)
	}
	defer cli.Close()

	// Domain target must pass ATYP=0x03 verbatim to server resolver
	c := socks5Connect(t, cli.ListenAddr(), "", "", "public.test.invalid", 80, AtypDomain)
	body, err := io.ReadAll(c)
	c.Close()
	if err != nil || string(body) != "domain-ok" {
		t.Fatalf("domain connect got %q err=%v", string(body), err)
	}
	if got, _ := resolvedHost.Load().(string); got != "public.test.invalid" {
		t.Fatalf("expected server LookupIP('public.test.invalid'), got %q", got)
	}

	// Blocked literal 10.0.0.1:80 and domain resolving to 127.0.0.1 must fail SOCKS5 connect
	for _, tc := range []struct {
		atyp byte
		host string
	}{
		{AtypIPv4, "10.0.0.1"},
		{AtypDomain, "loop.test.invalid"},
	} {
		raw, err := net.DialTimeout("tcp", cli.ListenAddr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial socks5: %v", err)
		}
		_, _ = raw.Write([]byte{0x05, 0x01, 0x00})
		_, _ = io.ReadFull(raw, make([]byte, 2))
		ab, _ := EncodeTargetAddr(TargetAddr{Atyp: tc.atyp, Host: tc.host, Port: 80})
		_, _ = raw.Write(append([]byte{0x05, 0x01, 0x00}, ab...))
		var rep [4]byte
		_, err = io.ReadFull(raw, rep[:])
		raw.Close()
		if err != nil || rep[1] == 0x00 {
			t.Fatalf("expected blocked target %s to fail, got rep=%x err=%v", tc.host, rep, err)
		}
	}
}

func TestStartClientFailsFastOnHandshakeReject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Case 1: unupdated server closes on MLB1
	plainLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen plain: %v", err)
	}
	defer plainLn.Close()
	go func() {
		for {
			c, err := plainLn.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	r0 := startMockSocksRoom(t, 0)
	start := time.Now()
	_, err = StartClient(ctx, ClientConfig{
		ListenAddr:   "127.0.0.1:0",
		TargetAddr:   plainLn.Addr().String(),
		UUID:         testUUID,
		RoomPorts:    []int{r0.port},
		ReadyTimeout: 5 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "bondmux handshake") {
		t.Fatalf("expected bondmux handshake error, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("expected fail-fast < 2s, took %v", time.Since(start))
	}

	// Case 2: wrong UUID against real Server
	srv, err := StartServer(ctx, ServerConfig{
		ListenAddr: "127.0.0.1:0",
		UUIDs:      []string{testUUID},
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	start = time.Now()
	_, err = StartClient(ctx, ClientConfig{
		ListenAddr:   "127.0.0.1:0",
		TargetAddr:   srv.Addr(),
		UUID:         "11111111-1111-1111-1111-111111111111",
		RoomPorts:    []int{r0.port},
		ReadyTimeout: 5 * time.Second,
	})
	if !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("expected ErrAuthFailed, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("expected auth fail-fast < 2s, took %v", time.Since(start))
	}
}

func TestStreamResetClosesBothDirectionsWithoutLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverReadDone := make(chan struct{})
	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	go func() {
		c, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		defer close(serverReadDone)
		_, _ = io.Copy(io.Discard, c)
	}()

	srv, err := StartServer(ctx, ServerConfig{
		ListenAddr:           "127.0.0.1:0",
		UUIDs:                []string{testUUID},
		AllowLoopbackForTest: true,
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	r0 := startMockSocksRoom(t, 0)
	cli, err := StartClient(ctx, ClientConfig{
		ListenAddr:   "127.0.0.1:0",
		TargetAddr:   srv.Addr(),
		UUID:         testUUID,
		RoomPorts:    []int{r0.port},
		ReadyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartClient: %v", err)
	}
	defer cli.Close()

	targetPort := uint16(targetLn.Addr().(*net.TCPAddr).Port)
	conn := socks5Connect(t, cli.ListenAddr(), "", "", "127.0.0.1", targetPort, AtypIPv4)
	_, _ = conn.Write([]byte("hello"))
	// Abruptly reset client side
	_ = conn.(*net.TCPConn).SetLinger(0)
	_ = conn.Close()

	select {
	case <-serverReadDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("server target socket did not close after abrupt client disconnect")
	}
}
