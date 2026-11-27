package mobile

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/pkg/bondmux"
)

func TestMobileBondMuxLifecycle(t *testing.T) {
	uuid := "2b9618c0-76da-4f8d-bf1f-3270d30ff6a3"

	// Reject non-loopback without credentials
	bad := NewBondMux()
	bad.SetListenHost("0.0.0.0")
	bad.SetListenPort(18080)
	bad.SetTarget("127.0.0.1:10000")
	bad.SetUUID(uuid)
	bad.SetRoomPortsCSV("18081")
	if err := bad.Start(1000); err == nil {
		_ = bad.Stop(1000)
		t.Fatalf("expected error for non-loopback host without credentials")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	go func() {
		c, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write([]byte("mobile-ok"))
	}()

	srv, err := bondmux.StartServer(ctx, bondmux.ServerConfig{
		ListenAddr:           "127.0.0.1:0",
		UUIDs:                []string{uuid},
		AllowLoopbackForTest: true,
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer srv.Close()

	// Mock room SOCKS5
	roomLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen room: %v", err)
	}
	defer roomLn.Close()
	go func() {
		for {
			c, err := roomLn.Accept()
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

	freeLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	listenPort := freeLn.Addr().(*net.TCPAddr).Port
	_ = freeLn.Close()

	rt := NewBondMux()
	rt.SetListenHost("127.0.0.1")
	rt.SetListenPort(int64(listenPort))
	rt.SetSocksCredentials("mobu", "mobp")
	rt.SetTarget(srv.Addr())
	rt.SetUUID(uuid)
	rt.SetRoomPortsCSV(strconv.Itoa(roomLn.Addr().(*net.TCPAddr).Port))
	rt.SetStreamWindow(1 << 20)
	rt.SetSessionWindow(12 << 20)

	if err := rt.Start(5000); err != nil {
		t.Fatalf("BondMuxRuntime.Start: %v", err)
	}
	if !rt.IsRunning() {
		t.Fatalf("expected IsRunning=true after Start")
	}

	c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(listenPort), 2*time.Second)
	if err != nil {
		t.Fatalf("dial mobile socks: %v", err)
	}
	_, _ = c.Write([]byte{0x05, 0x01, 0x02})
	var rep [2]byte
	_, _ = io.ReadFull(c, rep[:])
	_, _ = c.Write([]byte{0x01, 4, 'm', 'o', 'b', 'u', 4, 'm', 'o', 'b', 'p'})
	_, _ = io.ReadFull(c, rep[:])
	ab, _ := bondmux.EncodeTargetAddr(bondmux.TargetAddr{
		Atyp: bondmux.AtypIPv4,
		Host: "127.0.0.1",
		Port: uint16(targetLn.Addr().(*net.TCPAddr).Port),
	})
	_, _ = c.Write(append([]byte{0x05, 0x01, 0x00}, ab...))
	var crep [10]byte
	_, _ = io.ReadFull(c, crep[:])
	body, err := io.ReadAll(c)
	c.Close()
	if err != nil || string(body) != "mobile-ok" {
		t.Fatalf("mobile transfer got %q err=%v", string(body), err)
	}

	if err := rt.Stop(3000); err != nil {
		t.Fatalf("BondMuxRuntime.Stop: %v", err)
	}
	if rt.IsRunning() {
		t.Fatalf("expected IsRunning=false after Stop")
	}
}
