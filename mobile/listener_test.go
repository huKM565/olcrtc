// ai-generated: the whole file (tests for the session listener).
package mobile

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

// The host cannot read the runtime's log, so the moment a session is
// established - the client's "session opened" line - reaches it through a
// listener instead, naming the room. It fires for every session, so a
// failover to another room is visible as exactly that.
func TestSessionListenerNamesTheRoomOfEachSession(t *testing.T) {
	shortFailoverDelay(t)
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		cfg.OnSessionOpen("session-in-" + cfg.RoomURL)
		if cfg.RoomURL == testRoom {
			return errTestRun
		}
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		return ctx.Err()
	})
	listener := &recordingListener{}
	runtime.SetSessionListener(listener)
	if err := runtime.AddFailoverRoom(testStandby); err != nil {
		t.Fatalf("AddFailoverRoom() error = %v", err)
	}
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(2000); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	want := []string{
		testRoom + " session-in-" + testRoom,
		testStandby + " session-in-" + testStandby,
	}
	if got := listener.events(); !reflect.DeepEqual(got, want) {
		t.Fatalf("session events = %v, want %v", got, want)
	}
	if err := runtime.Stop(100); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

// A runtime with no listener still lets the client report sessions; the events
// go nowhere. A listener installed later hears the next one, without a restart.
func TestSessionListenerIsOptionalAndReplaceable(t *testing.T) {
	sessions := make(chan struct{}, 1)
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		cfg.OnSessionOpen("first")
		onReady("127.0.0.1:1080")
		<-sessions
		cfg.OnSessionOpen("second")
		<-ctx.Done()
		return ctx.Err()
	})
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(2000); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	listener := &recordingListener{}
	runtime.SetSessionListener(listener)
	sessions <- struct{}{}
	deadline := time.Now().Add(2 * time.Second)
	for len(listener.events()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got, want := listener.events(), []string{testRoom + " second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("session events = %v, want %v", got, want)
	}
	if err := runtime.Stop(100); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

// A session that opens while its generation is being stopped is not one the
// host should act on: the event is dropped rather than delivered late.
func TestSessionListenerIgnoresAStoppingGeneration(t *testing.T) {
	stopping := make(chan struct{})
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		<-stopping
		cfg.OnSessionOpen("late")
		return ctx.Err()
	})
	listener := &recordingListener{}
	runtime.SetSessionListener(listener)
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(2000); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	if err := runtime.Stop(1); !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("Stop() error = %v, want %v", err, ErrStopTimeout)
	}
	close(stopping)
	waitForState(t, runtime, "stopped")
	if got := listener.events(); len(got) != 0 {
		t.Fatalf("session events = %v, want none from a stopping generation", got)
	}
}

// ai-generated: replay of the latest room state when the host installs its listener late.
func TestRoomStateListenerReplaysLatestStateOnActivation(t *testing.T) {
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		cfg.OnRoomState(client.RoomJoined)
		cfg.OnRoomState(client.RelayReady)
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		return ctx.Err()
	})
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(2000); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	listener := &recordingRoomStateListener{}
	runtime.SetRoomStateListener(listener)
	if got, want := listener.events(), []string{testRoom + " relay-ready"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("room states after listener activation = %v, want %v", got, want)
	}
	runtime.RequestRoomState()
	if got, want := listener.events(), []string{testRoom + " relay-ready", testRoom + " relay-ready"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("room states after replay = %v, want %v", got, want)
	}
	if err := runtime.Stop(100); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

type recordingRoomStateListener struct {
	mu     sync.Mutex
	states []string
}

func (l *recordingRoomStateListener) OnRoomStateChanged(room, state string) {
	l.mu.Lock()
	l.states = append(l.states, room+" "+state)
	l.mu.Unlock()
}

func (l *recordingRoomStateListener) events() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.states...)
}

// ai-generated: the host receives roster counts without parsing engine logs.
func TestOccupancyListenerReceivesRoomCounts(t *testing.T) {
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		cfg.OnOccupancy(3)
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		return ctx.Err()
	})
	listener := &recordingOccupancyListener{}
	runtime.SetOccupancyListener(listener)
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(2000); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	if got, want := listener.events(), []string{testRoom + " 3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("occupancy events = %v, want %v", got, want)
	}
	if err := runtime.Stop(100); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

// ai-generated: regression test for replaying a cached roster after room activation.
func TestRequestRoomOccupancyReplaysLatestCount(t *testing.T) {
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		cfg.OnOccupancy(3)
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		return ctx.Err()
	})
	listener := &recordingOccupancyListener{}
	runtime.SetOccupancyListener(listener)
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(2000); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	runtime.RequestRoomOccupancy()
	if got, want := listener.events(), []string{testRoom + " 3", testRoom + " 3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("occupancy events = %v, want %v", got, want)
	}
	if err := runtime.Stop(100); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	runtime.RequestRoomOccupancy()
	if got, want := listener.events(), []string{testRoom + " 3", testRoom + " 3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("occupancy events after stop = %v, want %v", got, want)
	}
}

type recordingOccupancyListener struct {
	mu       sync.Mutex
	received []string
}

func (l *recordingOccupancyListener) OnRoomOccupancyChanged(room string, participants int) {
	l.mu.Lock()
	l.received = append(l.received, fmt.Sprintf("%s %d", room, participants))
	l.mu.Unlock()
}

func (l *recordingOccupancyListener) events() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.received...)
}

type recordingListener struct {
	mu     sync.Mutex
	opened []string
}

func (l *recordingListener) OnSessionOpened(room, sessionID string) {
	l.mu.Lock()
	l.opened = append(l.opened, room+" "+sessionID)
	l.mu.Unlock()
}

func (l *recordingListener) events() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.opened...)
}
