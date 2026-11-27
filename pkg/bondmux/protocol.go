package bondmux

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

const (
	Magic            = "MLB1"
	HeaderSize       = 20
	MaxPayload       = 16384
	DefaultChunkSize = 8192

	FrameOpenTCP byte = 0x01
	FrameOpenOK  byte = 0x02
	FrameData    byte = 0x03
	FrameAck     byte = 0x04
	FrameWindow  byte = 0x05
	FrameFin     byte = 0x06
	FrameRst     byte = 0x07
	FrameUDPPkt  byte = 0x08
	FramePing    byte = 0x09
	FramePong    byte = 0x0A

	FlagOpenFail byte = 0x01

	AtypIPv4   byte = 0x01
	AtypDomain byte = 0x03
	AtypIPv6   byte = 0x04

	DefaultStreamWindowDesktop  uint32 = 4 << 20
	DefaultSessionWindowDesktop uint32 = 32 << 20
	DefaultMaxStreamsDesktop           = 512

	DefaultStreamWindowMobile  uint32 = 1 << 20
	DefaultSessionWindowMobile uint32 = 12 << 20
	DefaultMaxStreamsMobile           = 256
)

var (
	ErrPayloadTooLarge = errors.New("bondmux: payload exceeds maximum size")
	ErrInvalidAddr     = errors.New("bondmux: invalid target address")
	ErrPipeClosed      = errors.New("bondmux: pipe writer closed")
)

type Frame struct {
	Type     byte
	Flags    byte
	StreamID uint32
	Offset   uint64
	ChunkID  uint32
	Payload  []byte
}

func (f Frame) IsHighPriority() bool {
	return f.Type != FrameData && f.Type != FrameUDPPkt
}

func WriteFrame(w io.Writer, f Frame) error {
	if len(f.Payload) > MaxPayload {
		return fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, len(f.Payload), MaxPayload)
	}
	buf := make([]byte, HeaderSize+len(f.Payload))
	buf[0] = f.Type
	buf[1] = f.Flags
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(f.Payload)))
	binary.BigEndian.PutUint32(buf[4:8], f.StreamID)
	binary.BigEndian.PutUint64(buf[8:16], f.Offset)
	binary.BigEndian.PutUint32(buf[16:20], f.ChunkID)
	copy(buf[HeaderSize:], f.Payload)
	_, err := w.Write(buf)
	return err
}

func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	plen := int(binary.BigEndian.Uint16(hdr[2:4]))
	if plen > MaxPayload {
		return Frame{}, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, plen, MaxPayload)
	}
	var payload []byte
	if plen > 0 {
		payload = make([]byte, plen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, err
		}
	}
	return Frame{
		Type:     hdr[0],
		Flags:    hdr[1],
		StreamID: binary.BigEndian.Uint32(hdr[4:8]),
		Offset:   binary.BigEndian.Uint64(hdr[8:16]),
		ChunkID:  binary.BigEndian.Uint32(hdr[16:20]),
		Payload:  payload,
	}, nil
}

type TargetAddr struct {
	Atyp byte
	Host string
	Port uint16
}

func EncodeTargetAddr(dst TargetAddr) ([]byte, error) {
	if dst.Host == "" || dst.Port == 0 {
		return nil, ErrInvalidAddr
	}
	switch dst.Atyp {
	case AtypIPv4:
		ip := net.ParseIP(dst.Host).To4()
		if ip == nil {
			return nil, ErrInvalidAddr
		}
		out := make([]byte, 1+4+2)
		out[0] = AtypIPv4
		copy(out[1:5], ip)
		binary.BigEndian.PutUint16(out[5:7], dst.Port)
		return out, nil
	case AtypIPv6:
		ip := net.ParseIP(dst.Host).To16()
		if ip == nil {
			return nil, ErrInvalidAddr
		}
		out := make([]byte, 1+16+2)
		out[0] = AtypIPv6
		copy(out[1:17], ip)
		binary.BigEndian.PutUint16(out[17:19], dst.Port)
		return out, nil
	case AtypDomain:
		if len(dst.Host) > 255 {
			return nil, ErrInvalidAddr
		}
		out := make([]byte, 1+1+len(dst.Host)+2)
		out[0] = AtypDomain
		out[1] = byte(len(dst.Host))
		copy(out[2:2+len(dst.Host)], dst.Host)
		binary.BigEndian.PutUint16(out[2+len(dst.Host):], dst.Port)
		return out, nil
	default:
		return nil, ErrInvalidAddr
	}
}

func DecodeTargetAddr(b []byte) (TargetAddr, int, error) {
	if len(b) < 2 {
		return TargetAddr{}, 0, ErrInvalidAddr
	}
	switch b[0] {
	case AtypIPv4:
		if len(b) < 7 {
			return TargetAddr{}, 0, ErrInvalidAddr
		}
		return TargetAddr{
			Atyp: AtypIPv4,
			Host: net.IP(b[1:5]).String(),
			Port: binary.BigEndian.Uint16(b[5:7]),
		}, 7, nil
	case AtypIPv6:
		if len(b) < 19 {
			return TargetAddr{}, 0, ErrInvalidAddr
		}
		return TargetAddr{
			Atyp: AtypIPv6,
			Host: net.IP(b[1:17]).String(),
			Port: binary.BigEndian.Uint16(b[17:19]),
		}, 19, nil
	case AtypDomain:
		dlen := int(b[1])
		total := 2 + dlen + 2
		if dlen == 0 || len(b) < total {
			return TargetAddr{}, 0, ErrInvalidAddr
		}
		return TargetAddr{
			Atyp: AtypDomain,
			Host: string(b[2 : 2+dlen]),
			Port: binary.BigEndian.Uint16(b[2+dlen : total]),
		}, total, nil
	default:
		return TargetAddr{}, 0, ErrInvalidAddr
	}
}

func EncodeOpenTCP(initialWindow uint32, dst TargetAddr) ([]byte, error) {
	addrBytes, err := EncodeTargetAddr(dst)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 4+len(addrBytes))
	binary.BigEndian.PutUint32(out[0:4], initialWindow)
	copy(out[4:], addrBytes)
	return out, nil
}

func DecodeOpenTCP(b []byte) (uint32, TargetAddr, error) {
	if len(b) < 6 {
		return 0, TargetAddr{}, ErrInvalidAddr
	}
	win := binary.BigEndian.Uint32(b[0:4])
	dst, _, err := DecodeTargetAddr(b[4:])
	return win, dst, err
}

func EncodeOpenOK(initialWindow uint32, errMsg string) []byte {
	if len(errMsg) > 512 {
		errMsg = errMsg[:512]
	}
	out := make([]byte, 4+len(errMsg))
	binary.BigEndian.PutUint32(out[0:4], initialWindow)
	copy(out[4:], errMsg)
	return out
}

func DecodeOpenOK(b []byte) (uint32, string, error) {
	if len(b) < 4 {
		return 0, "", io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint32(b[0:4]), string(b[4:]), nil
}

func EncodeAck(chunkIDs []uint32) []byte {
	if len(chunkIDs) > 64 {
		chunkIDs = chunkIDs[:64]
	}
	out := make([]byte, 2+4*len(chunkIDs))
	binary.BigEndian.PutUint16(out[0:2], uint16(len(chunkIDs)))
	for i, id := range chunkIDs {
		binary.BigEndian.PutUint32(out[2+4*i:6+4*i], id)
	}
	return out
}

func DecodeAck(b []byte) ([]uint32, error) {
	if len(b) < 2 {
		return nil, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(b[0:2]))
	if n > 64 || len(b) < 2+4*n {
		return nil, io.ErrUnexpectedEOF
	}
	ids := make([]uint32, n)
	for i := 0; i < n; i++ {
		ids[i] = binary.BigEndian.Uint32(b[2+4*i : 6+4*i])
	}
	return ids, nil
}

func EncodeUDPPkt(dst TargetAddr, data []byte) ([]byte, error) {
	addrBytes, err := EncodeTargetAddr(dst)
	if err != nil {
		return nil, err
	}
	if len(addrBytes)+len(data) > MaxPayload {
		return nil, ErrPayloadTooLarge
	}
	out := make([]byte, len(addrBytes)+len(data))
	copy(out, addrBytes)
	copy(out[len(addrBytes):], data)
	return out, nil
}

func DecodeUDPPkt(b []byte) (TargetAddr, []byte, error) {
	dst, n, err := DecodeTargetAddr(b)
	if err != nil {
		return TargetAddr{}, nil, err
	}
	return dst, b[n:], nil
}

type PipeWriter struct {
	conn      net.Conn
	ctrlCh    chan Frame
	dataCh    chan Frame
	done      chan struct{}
	closeOnce sync.Once
	onErr     func(error)
}

func NewPipeWriter(conn net.Conn, onErr func(error)) *PipeWriter {
	pw := &PipeWriter{
		conn:   conn,
		ctrlCh: make(chan Frame, 256),
		dataCh: make(chan Frame, 256),
		done:   make(chan struct{}),
		onErr:  onErr,
	}
	go pw.loop()
	return pw
}

func (pw *PipeWriter) Enqueue(f Frame) error {
	ch := pw.dataCh
	if f.IsHighPriority() {
		ch = pw.ctrlCh
	}
	select {
	case <-pw.done:
		return ErrPipeClosed
	case ch <- f:
		return nil
	}
}

func (pw *PipeWriter) TryEnqueueCtrl(f Frame) bool {
	select {
	case <-pw.done:
		return false
	case pw.ctrlCh <- f:
		return true
	default:
		return false
	}
}

func (pw *PipeWriter) Close() {
	pw.closeOnce.Do(func() {
		close(pw.done)
		_ = pw.conn.Close()
	})
}

func (pw *PipeWriter) loop() {
	for {
		var f Frame
		select {
		case <-pw.done:
			return
		case f = <-pw.ctrlCh:
		default:
			select {
			case <-pw.done:
				return
			case f = <-pw.ctrlCh:
			case f = <-pw.dataCh:
			}
		}
		if err := WriteFrame(pw.conn, f); err != nil {
			pw.Close()
			if pw.onErr != nil {
				pw.onErr(err)
			}
			return
		}
	}
}
