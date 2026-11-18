package main

// ai-generated: unit tests for validated SaluteJazz room provisioning output.

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRunWritesValidatedRoom(t *testing.T) {
	var stdout strings.Builder
	err := run(context.Background(), &stdout, func(context.Context) (string, error) {
		return "abc123:passw0rd", nil
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, want := stdout.String(), "abc123:passw0rd\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

func TestRunRejectsCreationFailureAndMalformedRooms(t *testing.T) {
	tests := []struct {
		name string
		room string
		err  error
	}{
		{name: "creation failure", err: errors.New("create failed")},
		{name: "uppercase", room: "ABC123:passw0rd"},
		{name: "missing password", room: "abc123"},
		{name: "extra separator", room: "abc123:passw0rd:extra"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout strings.Builder
			err := run(context.Background(), &stdout, func(context.Context) (string, error) {
				return test.room, test.err
			})
			if err == nil {
				t.Fatal("run succeeded, want error")
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want no room output", stdout.String())
			}
		})
	}
}
