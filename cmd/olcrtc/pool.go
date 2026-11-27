package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
	"github.com/openlibrecommunity/olcrtc/internal/client"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

const (
	poolStopPollInterval   = 100 * time.Millisecond
	poolRoomRestartDelay   = 2 * time.Second
	poolMaxInitialStagger  = 2 * time.Second
	poolStaggerStep        = 200 * time.Millisecond
	poolLogFilePermissions = 0o600
)

var (
	errPoolProfilesUnsupported = errors.New("pool mode does not support failover profiles")
	errPoolModeCncRequired     = errors.New("pool mode requires mode: cnc")
)

type poolRoomSpec struct {
	scfg    session.Config
	dataDir string
}

func runPool(runtimeDir string, configPaths []string) error {
	if runtimeDir == "" || len(configPaths) == 0 {
		return ErrConfigPathRequired
	}
	if err := os.MkdirAll(runtimeDir, 0o700); err != nil {
		return fmt.Errorf("create pool runtime dir: %w", err)
	}

	specs := make([]poolRoomSpec, 0, len(configPaths))
	anyDebug := false
	for _, path := range configPaths {
		loaded, err := loadConfig(path)
		if err != nil {
			return err
		}
		if len(loaded.profiles) > 0 {
			return fmt.Errorf("%s: %w", path, errPoolProfilesUnsupported)
		}
		scfg := session.ApplyDefaults(loaded.scfg)
		if scfg.Mode != session.ModeCnc {
			return fmt.Errorf("%s: %w", path, errPoolModeCncRequired)
		}
		if err := session.Validate(scfg); err != nil {
			return fmt.Errorf("validate pool config %s: %w", path, err)
		}
		if loaded.dataDir != "" {
			if err := loadNameOverrides(loaded.dataDir); err != nil {
				return err
			}
		}
		if loaded.debug {
			anyDebug = true
		}
		specs = append(specs, poolRoomSpec{scfg: scfg, dataDir: loaded.dataDir})
	}

	configureLogging(anyDebug)
	logger.Infof("olcrtc %s (pool rooms=%d)", buildVersion(), len(specs))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	stopFile := filepath.Join(runtimeDir, "stop")
	stopRequested := func() bool {
		_, err := os.Stat(stopFile)
		return err == nil
	}

	go func() {
		ticker := time.NewTicker(poolStopPollInterval)
		defer ticker.Stop()
		for {
			if stopRequested() {
				cancel()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-sigCh:
				logger.Info("Shutting down pool gracefully...")
				cancel()
				return
			case <-ticker.C:
			}
		}
	}()

	var wg sync.WaitGroup
	for i := range specs {
		roomIndex := i
		scfg := specs[i].scfg
		wg.Go(func() {
			runPoolRoom(ctx, runtimeDir, roomIndex, len(specs), scfg, stopRequested)
		})
	}

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		return nil
	case <-ctx.Done():
		select {
		case <-waitDone:
			return nil
		case <-time.After(shutdownGrace):
			logger.Warn("Pool shutdown timeout, forcing exit")
			return nil
		}
	}
}

func runPoolRoom(
	ctx context.Context,
	runtimeDir string,
	index int,
	total int,
	scfg session.Config,
	stopRequested func() bool,
) {
	if total > 4 && index > 0 {
		delay := min(time.Duration(index)*poolStaggerStep, poolMaxInitialStagger)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}

	logPath := filepath.Join(runtimeDir, fmt.Sprintf("room-%d.log", index+1))
	statePath := filepath.Join(runtimeDir, fmt.Sprintf("room-%d.state", index+1))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, poolLogFilePermissions)
	if err != nil {
		logger.Warnf("pool room %d open log: %v", index+1, err)
	} else {
		defer func() { _ = logFile.Close() }()
	}

	var logMu sync.Mutex
	writeRoomLog := func(line string) {
		if logFile == nil {
			return
		}
		logMu.Lock()
		_, _ = fmt.Fprintf(logFile, "%s\n", line)
		logMu.Unlock()
	}

	scfg.OnRoomState = func(state client.RoomState) {
		writeRoomLog(fmt.Sprintf("client: room state=%s", state))
	}
	scfg.OnOccupancy = func(participants int) {
		writeRoomLog(fmt.Sprintf("jitsi: room participants=%d", participants))
	}

	for {
		if ctx.Err() != nil || stopRequested() {
			return
		}
		_ = os.WriteFile(statePath, []byte("up\n"), poolLogFilePermissions)
		runErr := runSession(ctx, scfg)
		if ctx.Err() != nil || stopRequested() {
			return
		}
		_ = os.WriteFile(statePath, []byte("down\n"), poolLogFilePermissions)
		if runErr != nil {
			writeRoomLog(fmt.Sprintf("room %d ended: %v", index+1, runErr))
		}
		timer := time.NewTimer(poolRoomRestartDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}
