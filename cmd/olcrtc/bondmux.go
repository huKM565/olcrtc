package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/pkg/bondmux"
)

type bondmuxClientFileConfig struct {
	ListenAddr     string `json:"listen_addr"`
	SocksUser      string `json:"socks_user"`
	SocksPass      string `json:"socks_pass"`
	TargetAddr     string `json:"target_addr"`
	UUID           string `json:"uuid"`
	RoomPorts      []int  `json:"room_ports"`
	RoomUser       string `json:"room_user"`
	RoomPass       string `json:"room_pass"`
	ChunkSize      int    `json:"chunk_size,omitempty"`
	StreamWindow   uint32 `json:"stream_window,omitempty"`
	SessionWindow  uint32 `json:"session_window,omitempty"`
	MaxStreams     int    `json:"max_streams,omitempty"`
	ReadyTimeoutMs int    `json:"ready_timeout_ms,omitempty"`
	Debug          bool   `json:"debug,omitempty"`
}

type bondmuxServerFileConfig struct {
	Listen               string   `yaml:"listen"`
	Fallback             string   `yaml:"fallback"`
	UUIDs                []string `yaml:"uuids"`
	DNS                  string   `yaml:"dns,omitempty"`
	ChunkSize            int      `yaml:"chunk_size,omitempty"`
	StreamWindow         uint32   `yaml:"stream_window,omitempty"`
	SessionWindow        uint32   `yaml:"session_window,omitempty"`
	MaxStreams           int      `yaml:"max_streams,omitempty"`
	SessionGraceSec      int      `yaml:"session_grace_sec,omitempty"`
	AllowLoopbackForTest bool     `yaml:"allow_loopback_for_test,omitempty"`
	Debug                bool     `yaml:"debug,omitempty"`
}

func runBondMuxClientCmd(path string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runBondMuxClient(ctx, path)
}

func runBondMuxServerCmd(path string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runBondMuxServer(ctx, path)
}

func runBondMuxClient(ctx context.Context, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read bondmux client config: %w", err)
	}
	var cfg bondmuxClientFileConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("parse bondmux client config: %w", err)
	}
	configureLogging(cfg.Debug)
	timeout := time.Duration(cfg.ReadyTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	cli, err := bondmux.StartClient(ctx, bondmux.ClientConfig{
		ListenAddr:    cfg.ListenAddr,
		SocksUser:     cfg.SocksUser,
		SocksPass:     cfg.SocksPass,
		TargetAddr:    cfg.TargetAddr,
		UUID:          cfg.UUID,
		RoomPorts:     cfg.RoomPorts,
		RoomUser:      cfg.RoomUser,
		RoomPass:      cfg.RoomPass,
		ChunkSize:     cfg.ChunkSize,
		StreamWindow:  cfg.StreamWindow,
		SessionWindow: cfg.SessionWindow,
		MaxStreams:    cfg.MaxStreams,
		ReadyTimeout:  timeout,
		Logf:          logger.Infof,
	})
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	<-ctx.Done()
	return nil
}

func runBondMuxServer(ctx context.Context, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read bondmux server config: %w", err)
	}
	var cfg bondmuxServerFileConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("parse bondmux server config: %w", err)
	}
	configureLogging(cfg.Debug)
	grace := time.Duration(cfg.SessionGraceSec) * time.Second
	srv, err := bondmux.StartServer(ctx, bondmux.ServerConfig{
		ListenAddr:           cfg.Listen,
		FallbackAddr:         cfg.Fallback,
		UUIDs:                cfg.UUIDs,
		DNS:                  cfg.DNS,
		ChunkSize:            cfg.ChunkSize,
		StreamWindow:         cfg.StreamWindow,
		SessionWindow:        cfg.SessionWindow,
		MaxStreams:           cfg.MaxStreams,
		SessionGrace:         grace,
		AllowLoopbackForTest: cfg.AllowLoopbackForTest,
		Logf:                 logger.Infof,
	})
	if err != nil {
		return err
	}
	logger.Infof("bondmux server listening on %s (fallback=%s)", srv.Addr(), cfg.Fallback)
	defer func() { _ = srv.Close() }()
	<-ctx.Done()
	return nil
}
