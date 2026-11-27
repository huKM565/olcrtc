package mobile

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openlibrecommunity/olcrtc/pkg/bondmux"
)

type BondMuxRuntime struct {
	mu            sync.Mutex
	listenHost    string
	listenPort    int
	socksUser     string
	socksPass     string
	targetAddr    string
	uuid          string
	roomPortsCSV  string
	roomUser      string
	roomPass      string
	streamWindow  uint32
	sessionWindow uint32
	client        *bondmux.Client
	lastError     string
}

func NewBondMux() *BondMuxRuntime {
	return &BondMuxRuntime{
		listenHost:    "127.0.0.1",
		streamWindow:  bondmux.DefaultStreamWindowMobile,
		sessionWindow: bondmux.DefaultSessionWindowMobile,
	}
}

func (r *BondMuxRuntime) SetListenHost(host string) {
	r.mu.Lock()
	r.listenHost = strings.TrimSpace(host)
	r.mu.Unlock()
}

func (r *BondMuxRuntime) SetListenPort(port int64) {
	r.mu.Lock()
	r.listenPort = int(port)
	r.mu.Unlock()
}

func (r *BondMuxRuntime) SetSocksCredentials(user, pass string) {
	r.mu.Lock()
	r.socksUser = user
	r.socksPass = pass
	r.mu.Unlock()
}

func (r *BondMuxRuntime) SetTarget(addr string) {
	r.mu.Lock()
	r.targetAddr = strings.TrimSpace(addr)
	r.mu.Unlock()
}

func (r *BondMuxRuntime) SetUUID(uuid string) {
	r.mu.Lock()
	r.uuid = strings.TrimSpace(uuid)
	r.mu.Unlock()
}

func (r *BondMuxRuntime) SetRoomPortsCSV(csv string) {
	r.mu.Lock()
	r.roomPortsCSV = strings.TrimSpace(csv)
	r.mu.Unlock()
}

func (r *BondMuxRuntime) SetRoomCredentials(user, pass string) {
	r.mu.Lock()
	r.roomUser = user
	r.roomPass = pass
	r.mu.Unlock()
}

func (r *BondMuxRuntime) SetStreamWindow(bytes int64) {
	if bytes > 0 {
		r.mu.Lock()
		r.streamWindow = uint32(bytes)
		r.mu.Unlock()
	}
}

func (r *BondMuxRuntime) SetSessionWindow(bytes int64) {
	if bytes > 0 {
		r.mu.Lock()
		r.sessionWindow = uint32(bytes)
		r.mu.Unlock()
	}
}

func parsePortsCSV(csv string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		p, err := strconv.Atoi(part)
		if err != nil || p < 1 || p > 65535 {
			return nil, fmt.Errorf("%w: invalid room port %q", ErrInvalidConfig, part)
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: room ports are required", ErrInvalidConfig)
	}
	return out, nil
}

func (r *BondMuxRuntime) Start(readyTimeoutMillis int64) error {
	r.mu.Lock()
	if r.client != nil && r.client.IsRunning() {
		r.mu.Unlock()
		return ErrAlreadyRunning
	}
	host := r.listenHost
	if host == "" {
		host = "127.0.0.1"
	}
	port := r.listenPort
	if port < 1 || port > 65535 {
		r.mu.Unlock()
		return fmt.Errorf("%w: invalid SOCKS listen port", ErrInvalidConfig)
	}
	if !isLoopbackHost(host) && (r.socksUser == "" || r.socksPass == "") {
		r.mu.Unlock()
		return fmt.Errorf("%w: SOCKS credentials are required outside loopback", ErrInvalidConfig)
	}
	ports, err := parsePortsCSV(r.roomPortsCSV)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	timeout := time.Duration(readyTimeoutMillis) * time.Millisecond
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	cfg := bondmux.ClientConfig{
		ListenAddr:    net.JoinHostPort(host, strconv.Itoa(port)),
		SocksUser:     r.socksUser,
		SocksPass:     r.socksPass,
		TargetAddr:    r.targetAddr,
		UUID:          r.uuid,
		RoomPorts:     ports,
		RoomUser:      r.roomUser,
		RoomPass:      r.roomPass,
		StreamWindow:  r.streamWindow,
		SessionWindow: r.sessionWindow,
		MaxStreams:    bondmux.DefaultMaxStreamsMobile,
		ReadyTimeout:  timeout,
	}
	r.lastError = ""
	r.mu.Unlock()

	cli, err := bondmux.StartClient(context.Background(), cfg)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.lastError = err.Error()
		return err
	}
	r.client = cli
	return nil
}

func (r *BondMuxRuntime) Stop(_ int64) error {
	r.mu.Lock()
	cli := r.client
	r.client = nil
	r.mu.Unlock()
	if cli == nil {
		return nil
	}
	return cli.Close()
}

func (r *BondMuxRuntime) IsRunning() bool {
	r.mu.Lock()
	cli := r.client
	r.mu.Unlock()
	return cli != nil && cli.IsRunning()
}

func (r *BondMuxRuntime) Diagnostics() string {
	r.mu.Lock()
	cli := r.client
	lastErr := r.lastError
	r.mu.Unlock()
	if cli != nil {
		return cli.Diagnostics()
	}
	return lastErr
}
