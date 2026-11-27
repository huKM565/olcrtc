package bondmux

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

func TestFrameRoundTrip(t *testing.T) {
	types := []byte{
		FrameOpenTCP, FrameOpenOK, FrameData, FrameAck, FrameWindow,
		FrameFin, FrameRst, FrameUDPPkt, FramePing, FramePong,
	}
	var buf bytes.Buffer
	for i, typ := range types {
		payload := []byte("hello-bondmux")
		if typ == FramePing || typ == FramePong {
			payload = nil
		}
		orig := Frame{
			Type:     typ,
			Flags:    byte(i % 4),
			StreamID: uint32(100 + i),
			Offset:   uint64(i) * 8192,
			ChunkID:  uint32(1000 + i),
			Payload:  payload,
		}
		buf.Reset()
		if err := WriteFrame(&buf, orig); err != nil {
			t.Fatalf("WriteFrame(type=%d): %v", typ, err)
		}
		if gotLen, wantLen := buf.Len(), HeaderSize+len(payload); gotLen != wantLen {
			t.Fatalf("type=%d encoded len=%d want %d", typ, gotLen, wantLen)
		}
		got, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame(type=%d): %v", typ, err)
		}
		if got.Type != orig.Type || got.Flags != orig.Flags ||
			got.StreamID != orig.StreamID || got.Offset != orig.Offset ||
			got.ChunkID != orig.ChunkID || !bytes.Equal(got.Payload, orig.Payload) {
			t.Fatalf("roundtrip mismatch: got %+v want %+v", got, orig)
		}
	}

	buf.Reset()
	err := WriteFrame(&buf, Frame{Type: FrameData, Payload: make([]byte, MaxPayload+1)})
	if err == nil {
		t.Fatalf("expected error for payload > MaxPayload (%d)", MaxPayload)
	}
}

func TestHandshakeOKAndRejectAndFallback(t *testing.T) {
	goodUUID, err := ParseUUIDBytes("2b9618c0-76da-4f8d-bf1f-3270d30ff6a3")
	if err != nil {
		t.Fatalf("ParseUUIDBytes: %v", err)
	}
	badUUID, err := ParseUUIDBytes("11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatalf("ParseUUIDBytes bad: %v", err)
	}
	sessionID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	now := func() time.Time { return time.Unix(1700000000, 0) }

	t.Run("valid_uuid", func(t *testing.T) {
		cConn, sConn := net.Pipe()
		defer cConn.Close()
		defer sConn.Close()
		var offset atomic.Int64
		errCh := make(chan error, 1)
		go func() {
			errCh <- ClientHandshakeWithClock(cConn, goodUUID, sessionID, 7, &offset, now)
		}()
		gotSess, gotPipe, isMux, prefix, sErr := ServerHandshake(bufio.NewReader(sConn), sConn, [][16]byte{goodUUID}, now)
		if sErr != nil {
			t.Fatalf("ServerHandshake: %v", sErr)
		}
		if !isMux || gotSess != sessionID || gotPipe != 7 || len(prefix) != 0 {
			t.Fatalf("unexpected server result: isMux=%v sess=%x pipe=%d prefix=%x", isMux, gotSess, gotPipe, prefix)
		}
		if cErr := <-errCh; cErr != nil {
			t.Fatalf("ClientHandshake: %v", cErr)
		}
	})

	t.Run("wrong_uuid", func(t *testing.T) {
		cConn, sConn := net.Pipe()
		defer cConn.Close()
		defer sConn.Close()
		var offset atomic.Int64
		errCh := make(chan error, 1)
		go func() {
			errCh <- ClientHandshakeWithClock(cConn, badUUID, sessionID, 3, &offset, now)
		}()
		_, _, isMux, _, sErr := ServerHandshake(bufio.NewReader(sConn), sConn, [][16]byte{goodUUID}, now)
		if sErr == nil || !isMux {
			t.Fatalf("expected auth error with isMux=true, got isMux=%v err=%v", isMux, sErr)
		}
		cErr := <-errCh
		if !errors.Is(cErr, ErrAuthFailed) {
			t.Fatalf("expected ErrAuthFailed, got %v", cErr)
		}
	})

	t.Run("vless_fallback", func(t *testing.T) {
		cConn, sConn := net.Pipe()
		defer cConn.Close()
		defer sConn.Close()
		vlessBytes := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}
		go func() {
			_, _ = cConn.Write(vlessBytes)
		}()
		br := bufio.NewReader(sConn)
		_, _, isMux, prefix, sErr := ServerHandshake(br, sConn, [][16]byte{goodUUID}, now)
		if sErr != nil {
			t.Fatalf("ServerHandshake fallback err: %v", sErr)
		}
		if isMux {
			t.Fatalf("expected isMux=false for VLESS bytes")
		}
		if !bytes.Equal(prefix, []byte{0x00, 0x01, 0x02, 0x03}) {
			t.Fatalf("unexpected prefix: %x", prefix)
		}
		rest := make([]byte, 2)
		if _, err := br.Read(rest); err != nil {
			t.Fatalf("read remaining bytes: %v", err)
		}
		if !bytes.Equal(rest, []byte{0x04, 0x05}) {
			t.Fatalf("unexpected remaining bytes: %x", rest)
		}
	})
}

func TestHandshakeClockSkewRetry(t *testing.T) {
	uuid, err := ParseUUIDBytes("2b9618c0-76da-4f8d-bf1f-3270d30ff6a3")
	if err != nil {
		t.Fatalf("ParseUUIDBytes: %v", err)
	}
	sessionID := [16]byte{9, 8, 7}
	serverNow := func() time.Time { return time.Unix(1700001000, 0) }
	clientNow := func() time.Time { return time.Unix(1700000700, 0) } // -300s skew

	var offset atomic.Int64

	// First attempt hits StatusClockSkew and sets offset = +300
	c1, s1 := net.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- ClientHandshakeWithClock(c1, uuid, sessionID, 1, &offset, clientNow)
	}()
	_, _, isMux, _, sErr := ServerHandshake(bufio.NewReader(s1), s1, [][16]byte{uuid}, serverNow)
	c1.Close()
	s1.Close()
	if !isMux || !errors.Is(sErr, ErrClockSkew) {
		t.Fatalf("expected server ErrClockSkew, got isMux=%v err=%v", isMux, sErr)
	}
	if cErr := <-errCh; !errors.Is(cErr, ErrClockSkew) {
		t.Fatalf("expected client ErrClockSkew, got %v", cErr)
	}
	if got := offset.Load(); got != 300 {
		t.Fatalf("expected clockOffset=300, got %d", got)
	}

	// Automatic retry with updated offset succeeds
	c2, s2 := net.Pipe()
	defer c2.Close()
	defer s2.Close()
	go func() {
		errCh <- ClientHandshakeWithClock(c2, uuid, sessionID, 2, &offset, clientNow)
	}()
	gotSess, gotPipe, isMux, _, sErr := ServerHandshake(bufio.NewReader(s2), s2, [][16]byte{uuid}, serverNow)
	if sErr != nil || !isMux || gotSess != sessionID || gotPipe != 2 {
		t.Fatalf("retry failed on server: isMux=%v sess=%x pipe=%d err=%v", isMux, gotSess, gotPipe, sErr)
	}
	if cErr := <-errCh; cErr != nil {
		t.Fatalf("retry failed on client: %v", cErr)
	}
}

func TestReorderDedupWindowAndSessionCap(t *testing.T) {
	var sessionBytes atomic.Int64
	rb := NewReorderBuffer(1<<20, &sessionBytes, 12<<20)
	defer rb.Close()

	c1 := bytes.Repeat([]byte{'B'}, 8192)
	c2 := bytes.Repeat([]byte{'C'}, 8192)
	c0 := bytes.Repeat([]byte{'A'}, 8192)

	out, ok := rb.Push(8192, c1)
	if !ok || len(out) != 0 {
		t.Fatalf("offset 8192: ok=%v len=%d", ok, len(out))
	}
	out, ok = rb.Push(16384, c2)
	if !ok || len(out) != 0 {
		t.Fatalf("offset 16384: ok=%v len=%d", ok, len(out))
	}
	// Duplicate at 8192 is accepted with no duplicate output
	out, ok = rb.Push(8192, c1)
	if !ok || len(out) != 0 {
		t.Fatalf("duplicate 8192: ok=%v len=%d", ok, len(out))
	}
	// MarkFin before offset 0 arrives should not emit EOF yet
	if rb.MarkFin(24576) {
		t.Fatalf("expected MarkFin=false before contiguous bytes reach 24576")
	}
	out, ok = rb.Push(0, c0)
	if !ok {
		t.Fatalf("offset 0 rejected")
	}
	var merged []byte
	for _, b := range out {
		merged = append(merged, b...)
	}
	if len(merged) != 24576 {
		t.Fatalf("merged len=%d want 24576", len(merged))
	}
	if !rb.MarkFin(24576) {
		t.Fatalf("expected MarkFin=true once NextOffset==24576")
	}
	if sessionBytes.Load() != 0 {
		t.Fatalf("expected sessionBytes=0 after drain, got %d", sessionBytes.Load())
	}

	// Exceeding stream window (1 MiB) must be rejected
	if _, ok := rb.Push(24576+(1<<20)+1, []byte("x")); ok {
		t.Fatalf("expected rejection past stream window")
	}
	// Exceeding session cap must be rejected
	sessionBytes.Store(12 << 20)
	if _, ok := rb.Push(24576+8192, []byte("y")); ok {
		t.Fatalf("expected rejection past session cap")
	}
	sessionBytes.Store(0)
}

func TestSchedulerShufflesFastPipesAndAvoidsSlowPipe(t *testing.T) {
	sched := NewScheduler(rand.New(rand.NewSource(42)))
	p1 := NewPipe(1)
	p1.SetStatsForTest(20*time.Millisecond, 10<<20, 0)
	p2 := NewPipe(2)
	p2.SetStatsForTest(25*time.Millisecond, 10<<20, 0)
	p3 := NewPipe(3)
	p3.SetStatsForTest(200*time.Millisecond, 10<<20, 0)
	sched.RegisterPipe(p1)
	sched.RegisterPipe(p2)
	sched.RegisterPipe(p3)

	counts := map[uint16]int{}
	for i := 0; i < 100; i++ {
		p, ok := sched.PickPipe(DefaultChunkSize)
		if !ok {
			t.Fatalf("PickPipe returned false on iter %d", i)
		}
		counts[p.ID]++
	}
	if counts[1] <= 20 || counts[2] <= 20 {
		t.Fatalf("expected shuffling across fast pipes 1 and 2, got counts=%v", counts)
	}
	if counts[3] != 0 {
		t.Fatalf("expected 0 picks for slow pipe 3 while fast pipes have capacity, got %d", counts[3])
	}

	// Straggler rescue after max(3*medianRTT, 450ms)
	t0 := time.Unix(1700000000, 0)
	sched.TrackInFlight(77, Frame{Type: FrameData, StreamID: 1, Offset: 0, ChunkID: 77, Payload: []byte("abc")}, 1, t0)
	if rescued := sched.RescueExpired(t0.Add(200 * time.Millisecond)); len(rescued) != 0 {
		t.Fatalf("expected no rescue at 200ms, got %d", len(rescued))
	}
	rescued := sched.RescueExpired(t0.Add(500 * time.Millisecond))
	if len(rescued) != 1 || rescued[0].ChunkID != 77 {
		t.Fatalf("expected rescue of chunk 77 at 500ms, got %+v", rescued)
	}

	// Removing a pipe immediately returns its in-flight frames
	sched.TrackInFlight(88, Frame{Type: FrameData, StreamID: 1, Offset: 3, ChunkID: 88, Payload: []byte("def")}, 2, t0)
	dropped := sched.RemovePipe(2)
	if len(dropped) != 1 || dropped[0].ChunkID != 88 {
		t.Fatalf("expected dropped chunk 88 on RemovePipe(2), got %+v", dropped)
	}
}

func TestReorderSessionCapAppliesInOrder(t *testing.T) {
	var sessionBytes atomic.Int64
	sessionBytes.Store(12 << 20)
	rb := NewReorderBuffer(1<<20, &sessionBytes, 12<<20)
	defer rb.Close()
	if _, ok := rb.Push(0, []byte("in-order-over-cap")); ok {
		t.Fatalf("expected in-order push to be rejected when sessionBytes >= sessionCap")
	}
}

func TestFastestPipeAvoidsStalledPipe(t *testing.T) {
	sched := NewScheduler(rand.New(rand.NewSource(7)))
	p1 := NewPipe(1)
	p1.SetStatsForTest(10*time.Millisecond, 10<<20, 0)
	p2 := NewPipe(2)
	p2.SetStatsForTest(30*time.Millisecond, 10<<20, 0)
	sched.RegisterPipe(p1)
	sched.RegisterPipe(p2)

	t0 := time.Unix(1700000000, 0)
	sched.TrackInFlight(1, Frame{Type: FrameData, StreamID: 1, ChunkID: 1, Payload: []byte("x")}, 1, t0)
	// Pipe 1 stalls -> RescueExpired penalizes pipe 1
	rescued := sched.RescueExpired(t0.Add(600 * time.Millisecond))
	if len(rescued) != 1 {
		t.Fatalf("expected 1 rescued frame, got %d", len(rescued))
	}
	fp, ok := sched.FastestPipe()
	if !ok || fp.ID != 2 {
		t.Fatalf("expected FastestPipe to switch to healthy pipe 2 after pipe 1 stalled, got %v (ok=%v)", fp, ok)
	}
}

func TestReorderInOrderUnblocksWhenOverSessionCap(t *testing.T) {
	var sessionBytes atomic.Int64
	rb := NewReorderBuffer(1<<20, &sessionBytes, 16384)
	defer rb.Close()

	// Buffer an out-of-order chunk at 8192 and fill sessionBytes to cap.
	if out, ok := rb.Push(8192, bytes.Repeat([]byte{'B'}, 8192)); !ok || len(out) != 0 {
		t.Fatalf("out-of-order push failed: ok=%v len=%d", ok, len(out))
	}
	sessionBytes.Store(16384)

	// Missing in-order chunk at 0 must still be accepted to drain stuck chunks.
	out, ok := rb.Push(0, bytes.Repeat([]byte{'A'}, 8192))
	if !ok || len(out) != 2 {
		t.Fatalf("expected in-order push to unblock buffered chunk, ok=%v len=%d", ok, len(out))
	}
	if rb.BufferedBytes() != 0 {
		t.Fatalf("expected buffered bytes=0 after drain, got %d", rb.BufferedBytes())
	}
}

func TestSchedulerRescuePenalizesOncePerTickAndCapsRTT(t *testing.T) {
	sched := NewScheduler(rand.New(rand.NewSource(9)))
	p1 := NewPipe(1)
	p1.SetStatsForTest(50*time.Millisecond, 10<<20, 0)
	sched.RegisterPipe(p1)

	t0 := time.Unix(1700000000, 0)
	for i := uint32(1); i <= 10; i++ {
		sched.TrackInFlight(i, Frame{Type: FrameData, StreamID: 1, ChunkID: i, Payload: []byte("x")}, 1, t0)
	}
	rescued := sched.RescueExpired(t0.Add(600 * time.Millisecond))
	if len(rescued) != 10 {
		t.Fatalf("expected 10 rescued frames, got %d", len(rescued))
	}
	if p1.ewmaRTT != 500*time.Millisecond {
		t.Fatalf("expected single penalty ewmaRTT=500ms, got %v", p1.ewmaRTT)
	}
}

func TestSchedulerPickUDPPipePinsFlowAndFailsOver(t *testing.T) {
	sched := NewScheduler(rand.New(rand.NewSource(11)))
	p1 := NewPipe(1)
	p1.SetStatsForTest(20*time.Millisecond, 5<<20, 0)
	p2 := NewPipe(2)
	p2.SetStatsForTest(25*time.Millisecond, 5<<20, 0)
	sched.RegisterPipe(p1)
	sched.RegisterPipe(p2)

	first, ok := sched.PickUDPPipe(42, 0)
	if !ok {
		t.Fatalf("PickUDPPipe returned false")
	}
	for i := 0; i < 20; i++ {
		next, ok := sched.PickUDPPipe(42, first.ID)
		if !ok || next.ID != first.ID {
			t.Fatalf("expected flow 42 pinned to pipe %d, got %v", first.ID, next)
		}
	}
	// Stall pinned pipe -> must fail over to the other healthy pipe
	first.stalledUntil = time.Now().Add(3 * time.Second)
	other, ok := sched.PickUDPPipe(42, first.ID)
	if !ok || other.ID == first.ID {
		t.Fatalf("expected failover away from stalled pipe %d, got %v", first.ID, other)
	}
}

func TestEgressHappyEyeballsPrefersIPv4AndCachesDNS(t *testing.T) {
	var lookups atomic.Int32
	v6Dead := netip.MustParseAddr("2606:4700:4700::1111")
	v4Slow := netip.MustParseAddr("1.1.1.1")
	v4Fast := netip.MustParseAddr("8.8.8.8")

	var firstDialed atomic.Value
	dialer := newEgressDialer(
		"",
		false,
		func(ctx context.Context, host string) ([]netip.Addr, error) {
			lookups.Add(1)
			return []netip.Addr{v6Dead, v4Slow, v4Fast}, nil
		},
		func(ctx context.Context, addr netip.Addr, port uint16) (net.Conn, error) {
			firstDialed.CompareAndSwap(nil, addr)
			if addr == v4Slow {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			c1, c2 := net.Pipe()
			_ = c2.Close()
			return c1, nil
		},
	)

	dst := TargetAddr{Atyp: AtypDomain, Host: "video.example.com", Port: 443}
	start := time.Now()
	conn, err := dialer.dialTCP(context.Background(), dst)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("dialTCP failed: %v", err)
	}
	_ = conn.Close()
	if got := firstDialed.Load().(netip.Addr); got != v4Slow {
		t.Fatalf("expected IPv4 %v tried before IPv6 %v, got %v", v4Slow, v6Dead, got)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("expected Happy Eyeballs failover < 2s, took %v", elapsed)
	}
	// Second resolve and resolveUDPFast must hit DNS cache without calling LookupIP again.
	uaddr, ok := dialer.resolveUDPFast(dst)
	if !ok || uaddr.IP.String() != "1.1.1.1" {
		t.Fatalf("expected cached IPv4 1.1.1.1 from resolveUDPFast, got %v ok=%v", uaddr, ok)
	}
	if lookups.Load() != 1 {
		t.Fatalf("expected 1 DNS lookup with caching, got %d", lookups.Load())
	}
}
