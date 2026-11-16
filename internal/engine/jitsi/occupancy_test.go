// ai-generated: tests for the room occupancy reporter.
package jitsi

import (
	"context"
	"reflect"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

func TestRoomParticipantsCountsTheLocalEndpoint(t *testing.T) {
	if got := roomParticipants(nil); got != 1 {
		t.Fatalf("participants without remote endpoints = %d, want 1", got)
	}
	if got := roomParticipants([]string{"relay", "other"}); got != 3 {
		t.Fatalf("participants with two remote endpoints = %d, want 3", got)
	}
}

func TestOccupancyReporterEmitsOnlyChanges(t *testing.T) {
	var got []int
	reporter := occupancyReporter{onChange: func(participants int) {
		got = append(got, participants)
	}}
	for _, participants := range []int{1, 1, 2, 2, 1} {
		reporter.report(participants)
	}
	if want := []int{1, 2, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("occupancy events = %v, want %v", got, want)
	}
}

func TestOccupancyWithoutAJitsiSessionIsUnknown(t *testing.T) {
	var got []int
	s := &Session{occupancy: occupancyReporter{last: -1, onChange: func(participants int) {
		got = append(got, participants)
	}}}
	s.reportOccupancy()
	if want := []int{0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("occupancy without jSess = %v, want unknown %v", got, want)
	}
}

func TestNewKeepsTheOccupancyCallback(t *testing.T) {
	sess, err := New(context.Background(), engine.Config{
		URL: testHost, Extra: map[string]string{credentialKeyRoom: testRoom},
		OnOccupancy: func(int) {},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	if sess.(*Session).occupancy.onChange == nil {
		t.Fatal("the Jitsi session dropped OnOccupancy")
	}
}
