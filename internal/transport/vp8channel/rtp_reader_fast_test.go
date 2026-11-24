package vp8channel

import (
	"testing"

	"github.com/pion/rtp"
)

func TestReorderOrderedFastPathDeliversImmediately(t *testing.T) {
	t.Parallel()
	rb := newReorderBuffer()
	var delivered []uint16
	for seq := uint16(65534); seq != 4; seq++ {
		pkt := &rtp.Packet{
			Header:  rtp.Header{SequenceNumber: seq},
			Payload: []byte{byte(seq & 0xff)},
		}
		var got *rtp.Packet
		rb.push(pkt, func(p *rtp.Packet) {
			got = p
			delivered = append(delivered, p.SequenceNumber)
		})
		if got != pkt {
			t.Fatalf("seq %d: expected direct delivery without clone", seq)
		}
		if len(rb.pkts) != 0 || len(rb.free) != 0 {
			t.Fatalf("seq %d: unexpected buffer use pkts=%d free=%d", seq, len(rb.pkts), len(rb.free))
		}
		if !rb.gapSince.IsZero() {
			t.Fatalf("seq %d: gapSince must remain zero", seq)
		}
	}
	want := []uint16{65534, 65535, 0, 1, 2, 3}
	if len(delivered) != len(want) {
		t.Fatalf("delivered = %v, want %v", delivered, want)
	}
	for i := range want {
		if delivered[i] != want[i] {
			t.Fatalf("delivered[%d] = %d, want %d", i, delivered[i], want[i])
		}
	}
}

func TestReorderFastPathThenGapAndRecovery(t *testing.T) {
	t.Parallel()
	rb := newReorderBuffer()
	var delivered []uint16
	deliver := func(p *rtp.Packet) { delivered = append(delivered, p.SequenceNumber) }

	rb.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 10}, Payload: []byte{10}}, deliver)
	rb.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 12}, Payload: []byte{12}}, deliver)
	if len(delivered) != 1 || rb.gapSince.IsZero() {
		t.Fatalf("after gap: delivered=%v gapSince=%v", delivered, rb.gapSince)
	}
	rb.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 12}, Payload: []byte{12}}, deliver)
	rb.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 9}, Payload: []byte{9}}, deliver)
	rb.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 11}, Payload: []byte{11}}, deliver)
	if !rb.gapSince.IsZero() || len(rb.pkts) != 0 {
		t.Fatalf("after gap filled: pkts=%d gapSince=%v", len(rb.pkts), rb.gapSince)
	}
	pkt13 := &rtp.Packet{Header: rtp.Header{SequenceNumber: 13}, Payload: []byte{13}}
	var got13 *rtp.Packet
	rb.push(pkt13, func(p *rtp.Packet) {
		got13 = p
		deliver(p)
	})
	if got13 != pkt13 {
		t.Fatal("expected fast path after gap drained")
	}
	want := []uint16{10, 11, 12, 13}
	if len(delivered) != len(want) {
		t.Fatalf("delivered = %v, want %v", delivered, want)
	}
	for i := range want {
		if delivered[i] != want[i] {
			t.Fatalf("delivered[%d] = %d, want %d", i, delivered[i], want[i])
		}
	}
}
