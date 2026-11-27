package bondmux

import (
	"bufio"
	"bytes"
	"errors"
	"math/rand"
	"net"
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
