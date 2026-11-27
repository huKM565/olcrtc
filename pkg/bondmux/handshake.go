package bondmux

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	HandshakeSize        = 62
	HandshakeVersion     = 0x01
	StatusOK         byte = 0x00
	StatusAuthFail   byte = 0x01
	StatusClockSkew  byte = 0x02

	MaxClockSkew     = 120 * time.Second
	HandshakeTimeout = 5 * time.Second
)

var (
	ErrAuthFailed   = errors.New("bondmux handshake: auth failed")
	ErrClockSkew    = errors.New("bondmux handshake: clock skew")
	ErrBadHandshake = errors.New("bondmux handshake: invalid response")
)

func ParseUUIDBytes(raw string) ([16]byte, error) {
	u, err := uuid.Parse(raw)
	if err != nil {
		return [16]byte{}, fmt.Errorf("bondmux: parse uuid: %w", err)
	}
	return [16]byte(u), nil
}

func computeHandshakeMAC(uuidBytes [16]byte, header30 []byte) [32]byte {
	mac := hmac.New(sha256.New, uuidBytes[:])
	_, _ = mac.Write(header30)
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func ClientHandshake(conn net.Conn, uuidBytes [16]byte, sessionID [16]byte, pipeID uint16, offset *atomic.Int64) error {
	return ClientHandshakeWithClock(conn, uuidBytes, sessionID, pipeID, offset, time.Now)
}

func ClientHandshakeWithClock(
	conn net.Conn,
	uuidBytes [16]byte,
	sessionID [16]byte,
	pipeID uint16,
	offset *atomic.Int64,
	now func() time.Time,
) error {
	var msg [HandshakeSize]byte
	copy(msg[0:4], Magic)
	copy(msg[4:20], sessionID[:])
	binary.BigEndian.PutUint16(msg[20:22], pipeID)
	localUnix := now().Unix()
	adjUnix := localUnix
	if offset != nil {
		adjUnix += offset.Load()
	}
	binary.BigEndian.PutUint64(msg[22:30], uint64(adjUnix))
	sum := computeHandshakeMAC(uuidBytes, msg[0:30])
	copy(msg[30:62], sum[:])

	if _, err := conn.Write(msg[:]); err != nil {
		return fmt.Errorf("%w: write: %v", ErrBadHandshake, err)
	}

	var resp [2]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		return fmt.Errorf("%w: read: %v", ErrBadHandshake, err)
	}
	if resp[0] != HandshakeVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrBadHandshake, resp[0])
	}
	switch resp[1] {
	case StatusOK:
		return nil
	case StatusAuthFail:
		return ErrAuthFailed
	case StatusClockSkew:
		var tsBuf [8]byte
		if _, err := io.ReadFull(conn, tsBuf[:]); err == nil && offset != nil {
			serverUnix := int64(binary.BigEndian.Uint64(tsBuf[:]))
			offset.Store(serverUnix - localUnix)
		}
		return ErrClockSkew
	default:
		return fmt.Errorf("%w: status %d", ErrBadHandshake, resp[1])
	}
}

func ServerHandshake(
	r *bufio.Reader,
	w net.Conn,
	uuids [][16]byte,
	now func() time.Time,
) ([16]byte, uint16, bool, []byte, error) {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return [16]byte{}, 0, false, nil, err
	}
	if string(magic[:]) != Magic {
		prefix := make([]byte, 4)
		copy(prefix, magic[:])
		return [16]byte{}, 0, false, prefix, nil
	}

	var rest [HandshakeSize - 4]byte
	if _, err := io.ReadFull(r, rest[:]); err != nil {
		return [16]byte{}, 0, true, nil, fmt.Errorf("%w: %v", ErrBadHandshake, err)
	}

	var full [HandshakeSize]byte
	copy(full[0:4], magic[:])
	copy(full[4:], rest[:])

	var sessionID [16]byte
	copy(sessionID[:], full[4:20])
	pipeID := binary.BigEndian.Uint16(full[20:22])
	clientUnix := int64(binary.BigEndian.Uint64(full[22:30]))
	serverUnix := now().Unix()

	delta := serverUnix - clientUnix
	if delta < 0 {
		delta = -delta
	}
	if time.Duration(delta)*time.Second > MaxClockSkew {
		var resp [10]byte
		resp[0] = HandshakeVersion
		resp[1] = StatusClockSkew
		binary.BigEndian.PutUint64(resp[2:10], uint64(serverUnix))
		_, _ = w.Write(resp[:])
		return sessionID, pipeID, true, nil, ErrClockSkew
	}

	matched := false
	for _, u := range uuids {
		expected := computeHandshakeMAC(u, full[0:30])
		if hmac.Equal(expected[:], full[30:62]) {
			matched = true
			break
		}
	}
	if !matched {
		_, _ = w.Write([]byte{HandshakeVersion, StatusAuthFail})
		return sessionID, pipeID, true, nil, ErrAuthFailed
	}

	if _, err := w.Write([]byte{HandshakeVersion, StatusOK}); err != nil {
		return sessionID, pipeID, true, nil, err
	}
	return sessionID, pipeID, true, nil, nil
}
