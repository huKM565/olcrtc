// ai-generated: the whole file (the session-open hook against a real client).
package client

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	enginebuiltin "github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/server"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/transport/datachannel"
)

// The hook fires once, with the id the server assigned, before the SOCKS
// listener is reported ready: a host acting on it can already send through
// the session it names.
func TestOnSessionOpenReportsTheEstablishedSession(t *testing.T) {
	transport.Register("datachannel", datachannel.New)
	room := &listenerTestRoom{sessions: make(map[*listenerTestSession]struct{})}
	providerName := "listener-test-" + t.Name()
	enginebuiltin.Register(providerName, func(_ context.Context, cfg enginebuiltin.Config) (engine.Session, error) {
		return room.newSession(cfg.OnData), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Run(ctx, server.Config{
			Transport: "datachannel", Provider: providerName, RoomURL: "room", KeyHex: listenerTestKey,
		})
	}()
	room.waitConnected(t, 1)

	var mu sync.Mutex
	var opened []string
	var states []RoomState
	address := make(chan string, 1)
	clientErr := make(chan error, 1)
	go func() {
		clientErr <- RunWithAddress(ctx, Config{
			Transport: "datachannel", Provider: providerName, RoomURL: "room", KeyHex: listenerTestKey,
			LocalAddr: "127.0.0.1:0", DeviceID: "session-open-client",
			OnSessionOpen: func(sessionID string) {
				mu.Lock()
				opened = append(opened, sessionID)
				mu.Unlock()
			},
			OnRoomState: func(state RoomState) {
				mu.Lock()
				states = append(states, state)
				mu.Unlock()
			},
		}, func(actualAddr string) { address <- actualAddr })
	}()
	waitListenerAddress(t, address, clientErr, serverErr)

	mu.Lock()
	got := append([]string(nil), opened...)
	gotStates := append([]RoomState(nil), states...)
	mu.Unlock()
	if len(got) != 1 || got[0] == "" {
		t.Fatalf("session-open calls = %q, want exactly one carrying the session id", got)
	}
	if want := []RoomState{RoomJoined, RelayReady}; !reflect.DeepEqual(gotStates, want) {
		t.Fatalf("room states = %v, want %v", gotStates, want)
	}

	cancel()
	waitListenerRun(t, "client", clientErr)
	waitListenerRun(t, "server", serverErr)
}

// ai-generated: a provider refusal is distinct from a joined room without a relay handshake.
func TestRoomStateReportsJoinFailure(t *testing.T) {
	transport.Register("datachannel", datachannel.New)
	providerName := "listener-test-" + t.Name()
	enginebuiltin.Register(providerName, func(_ context.Context, _ enginebuiltin.Config) (engine.Session, error) {
		return nil, errors.New("provider join refused")
	})
	var states []RoomState

	err := Run(context.Background(), Config{
		Transport: "datachannel", Provider: providerName, RoomURL: "room", KeyHex: listenerTestKey,
		LocalAddr: "127.0.0.1:0", DeviceID: "join-failure-client",
		OnRoomState: func(state RoomState) { states = append(states, state) },
	})
	if err == nil {
		t.Fatal("Run() error = nil, want provider join failure")
	}
	if want := []RoomState{RoomJoinFailed}; !reflect.DeepEqual(states, want) {
		t.Fatalf("room states = %v, want %v", states, want)
	}
}

// ai-generated: joining the room must not be mistaken for establishing the relay session.
func TestRoomStateReportsRelayFailureAfterJoin(t *testing.T) {
	transport.Register("datachannel", datachannel.New)
	room := &listenerTestRoom{sessions: make(map[*listenerTestSession]struct{})}
	providerName := "listener-test-" + t.Name()
	enginebuiltin.Register(providerName, func(_ context.Context, cfg enginebuiltin.Config) (engine.Session, error) {
		return room.newSession(cfg.OnData), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var states []RoomState

	err := Run(ctx, Config{
		Transport: "datachannel", Provider: providerName, RoomURL: "room", KeyHex: listenerTestKey,
		LocalAddr: "127.0.0.1:0", DeviceID: "relay-failure-client",
		OnRoomState: func(state RoomState) { states = append(states, state) },
	})
	if err == nil {
		t.Fatal("Run() error = nil, want handshake failure")
	}
	if want := []RoomState{RoomJoined, RelayFailed}; !reflect.DeepEqual(states, want) {
		t.Fatalf("room states = %v, want %v", states, want)
	}
}
