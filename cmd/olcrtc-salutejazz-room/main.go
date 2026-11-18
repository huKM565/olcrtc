// Package main provisions one anonymous SaluteJazz room for local deployment tooling.
package main

// ai-generated: the whole command, including validation and provider wiring.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	authSaluteJazz "github.com/openlibrecommunity/olcrtc/internal/auth/salutejazz"
)

const createTimeout = 30 * time.Second

var (
	errInvalidRoom = errors.New("invalid SaluteJazz room reference")
	roomRefRE      = regexp.MustCompile(`^[a-z0-9]{4,16}:[a-z0-9]{4,16}$`)
)

// ai-generated: run validates the provisioned room before making it observable.
func run(ctx context.Context, stdout io.Writer, createRoom func(context.Context) (string, error)) error {
	room, err := createRoom(ctx)
	if err != nil {
		return fmt.Errorf("create SaluteJazz room: %w", err)
	}
	if !roomRefRE.MatchString(room) {
		return errInvalidRoom
	}
	if _, err := fmt.Fprintln(stdout, room); err != nil {
		return fmt.Errorf("write SaluteJazz room: %w", err)
	}
	return nil
}

func main() {
	os.Exit(mainWithExit())
}

func mainWithExit() int {
	ctx, cancel := context.WithTimeout(context.Background(), createTimeout)
	defer cancel()
	provider := authSaluteJazz.New()
	if err := run(ctx, os.Stdout, func(ctx context.Context) (string, error) {
		return provider.CreateRoom(ctx, auth.Config{})
	}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
