package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/pkg/bondmux"
)

func TestRunBondMuxServerAndClientFlags(t *testing.T) {
	dir := t.TempDir()
	uuid := "2b9618c0-76da-4f8d-bf1f-3270d30ff6a3"

	// Target TCP server
	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	go func() {
		for {
			c, err := targetLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = conn.Write([]byte("cli-bondmux-ok"))
			}(c)
		}
	}()

	srvPort := freeTCPPort(t)
	cliPort := freeTCPPort(t)
	roomPort := startTestSocksRoom(t)

	srvYAML := "listen: 127.0.0.1:" + strconv.Itoa(srvPort) + "\n" +
		"fallback: 127.0.0.1:1\n" +
		"uuids:\n  - " + uuid + "\n" +
		"allow_loopback_for_test: true\n"
	srvPath := filepath.Join(dir, "server.yaml")
	if err := os.WriteFile(srvPath, []byte(srvYAML), 0o600); err != nil {
		t.Fatalf("write server yaml: %v", err)
	}

	cliCfg := bondmuxClientFileConfig{
		ListenAddr:     "127.0.0.1:" + strconv.Itoa(cliPort),
		SocksUser:      "u1",
		SocksPass:      "p1",
		TargetAddr:     "127.0.0.1:" + strconv.Itoa(srvPort),
		UUID:           uuid,
		RoomPorts:      []int{roomPort},
		ReadyTimeoutMs: 5000,
	}
	cliJSON, _ := json.Marshal(cliCfg)
	cliPath := filepath.Join(dir, "client.json")
	if err := os.WriteFile(cliPath, cliJSON, 0o600); err != nil {
		t.Fatalf("write client json: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvErr := make(chan error, 1)
	go func() { srvErr <- runBondMuxServer(ctx, srvPath) }()
	waitPortOpen(t, "127.0.0.1:"+strconv.Itoa(srvPort))

	cliErr := make(chan error, 1)
	go func() { cliErr <- runBondMuxClient(ctx, cliPath) }()
	waitPortOpen(t, "127.0.0.1:"+strconv.Itoa(cliPort))

	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(cliPort), 2*time.Second)
	if err != nil {
		t.Fatalf("dial client socks: %v", err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte{0x05, 0x01, 0x02})
	var rep [2]byte
	if _, err := io.ReadFull(conn, rep[:]); err != nil || rep[1] != 0x02 {
		t.Fatalf("auth method: %x err=%v", rep, err)
	}
	_, _ = conn.Write([]byte{0x01, 2, 'u', '1', 2, 'p', '1'})
	if _, err := io.ReadFull(conn, rep[:]); err != nil || rep[1] != 0x00 {
		t.Fatalf("auth reply: %x err=%v", rep, err)
	}
	ab, _ := bondmux.EncodeTargetAddr(bondmux.TargetAddr{
		Atyp: bondmux.AtypIPv4,
		Host: "127.0.0.1",
		Port: uint16(targetLn.Addr().(*net.TCPAddr).Port),
	})
	_, _ = conn.Write(append([]byte{0x05, 0x01, 0x00}, ab...))
	var crep [10]byte
	if _, err := io.ReadFull(conn, crep[:]); err != nil || crep[1] != 0x00 {
		t.Fatalf("connect reply: %x err=%v", crep, err)
	}
	body, err := io.ReadAll(conn)
	if err != nil || string(body) != "cli-bondmux-ok" {
		t.Fatalf("got %q err=%v", string(body), err)
	}

	cancel()
	if err := <-cliErr; err != nil {
		t.Fatalf("client exit err: %v", err)
	}
	if err := <-srvErr; err != nil {
		t.Fatalf("server exit err: %v", err)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	p := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return p
}

func waitPortOpen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("port %s did not open in time", addr)
}

func startTestSocksRoom(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock room: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				var h [2]byte
				if _, err := io.ReadFull(conn, h[:]); err != nil {
					return
				}
				_, _ = io.ReadFull(conn, make([]byte, h[1]))
				_, _ = conn.Write([]byte{0x05, 0x00})
				var r [4]byte
				if _, err := io.ReadFull(conn, r[:]); err != nil {
					return
				}
				var ip [4]byte
				_, _ = io.ReadFull(conn, ip[:])
				var pb [2]byte
				_, _ = io.ReadFull(conn, pb[:])
				port := int(pb[0])<<8 | int(pb[1])
				dst, err := net.DialTimeout("tcp", net.JoinHostPort(net.IP(ip[:]).String(), strconv.Itoa(port)), 2*time.Second)
				if err != nil {
					return
				}
				defer dst.Close()
				_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0})
				go func() { _, _ = io.Copy(dst, conn); _ = dst.Close() }()
				_, _ = io.Copy(conn, dst)
			}(c)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}
