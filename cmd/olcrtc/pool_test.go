package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
	"github.com/openlibrecommunity/olcrtc/internal/client"
)

func writePoolTestConfig(t *testing.T, dir string, name string, port int) string {
	t.Helper()
	path := filepath.Join(dir, name)
	body := fmt.Sprintf(`mode: cnc
auth:
  provider: jitsi
net:
  transport: datachannel
  dns: 8.8.8.8:53
room:
  id: https://meet.example.org/room-%d
crypto:
  key: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
socks:
  host: 127.0.0.1
  port: %d
`, port, port)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestRunPoolRunsAllRoomsLogsEventsRestartsAndStopsOnStopFile(t *testing.T) {
	dir := t.TempDir()
	runtimeDir := filepath.Join(dir, "runtime")
	var configs []string
	for i := range 2 {
		configs = append(configs, writePoolTestConfig(t, dir, fmt.Sprintf("r-%d.yaml", i+1), 21081+i))
	}

	orig := runSession
	defer func() { runSession = orig }()

	var mu sync.Mutex
	attempts := make(map[int]int)
	var readyCount atomic.Int32
	runSession = func(ctx context.Context, cfg session.Config) error {
		mu.Lock()
		attempts[cfg.SOCKSPort]++
		attempt := attempts[cfg.SOCKSPort]
		mu.Unlock()
		if cfg.SOCKSPort == 21081 && attempt == 1 {
			return errors.New("transient failure")
		}
		if cfg.OnRoomState != nil {
			cfg.OnRoomState(client.RoomJoined)
			cfg.OnRoomState(client.RelayReady)
		}
		if cfg.OnOccupancy != nil {
			cfg.OnOccupancy(2)
		}
		readyCount.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}

	done := make(chan error, 1)
	go func() {
		done <- runWithArgs(append([]string{"-pool", runtimeDir}, configs...))
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if readyCount.Load() >= 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if got := readyCount.Load(); got < 2 {
		t.Fatalf("readyCount = %d, want >= 2", got)
	}

	for i := range 2 {
		stateBytes, err := os.ReadFile(filepath.Join(runtimeDir, fmt.Sprintf("room-%d.state", i+1)))
		if err != nil {
			t.Fatalf("read room-%d.state: %v", i+1, err)
		}
		if got := strings.TrimSpace(string(stateBytes)); got != "up" {
			t.Fatalf("room-%d.state = %q, want up", i+1, got)
		}
		logBytes, err := os.ReadFile(filepath.Join(runtimeDir, fmt.Sprintf("room-%d.log", i+1)))
		if err != nil {
			t.Fatalf("read room-%d.log: %v", i+1, err)
		}
		logText := string(logBytes)
		if !strings.Contains(logText, "client: room state=relay-ready") {
			t.Fatalf("room-%d.log missing relay-ready: %s", i+1, logText)
		}
		if !strings.Contains(logText, "jitsi: room participants=2") {
			t.Fatalf("room-%d.log missing participants: %s", i+1, logText)
		}
	}

	if err := os.WriteFile(filepath.Join(runtimeDir, "stop"), nil, 0o600); err != nil {
		t.Fatalf("write stop file: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithArgs(-pool) error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runWithArgs(-pool) did not stop after stop file")
	}
}
