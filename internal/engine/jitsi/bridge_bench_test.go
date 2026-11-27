package jitsi

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/zarazaex69/j"
)

func BenchmarkEndpointMessageMarshal12KiB(b *testing.B) {
	payload := make([]byte, 12*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for range b.N {
		out, err := json.Marshal(newEndpointMessage("abc12345", payload))
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty marshal output")
		}
	}
}

func BenchmarkDecodeRaw12KiB(b *testing.B) {
	payload := make([]byte, 12*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	enc := base64.StdEncoding.EncodeToString(payload)
	msg := j.BridgeMessage{
		Class: colibriClassEndpointMessage,
		From:  "abc12345",
		Fields: map[string]any{
			"msgPayload": map[string]any{"raw": enc},
		},
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for range b.N {
		out := decodeRaw(msg)
		if len(out) != len(payload) {
			b.Fatalf("len = %d", len(out))
		}
	}
}

func BenchmarkDecodeRawJSON12KiB(b *testing.B) {
	payload := make([]byte, 12*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	rawJSON := []byte(`{"colibriClass":"EndpointMessage","from":"abc12345","msgPayload":{"raw":"` +
		base64.StdEncoding.EncodeToString(payload) + `"}}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for range b.N {
		from, out, ok := decodeRawJSON(rawJSON)
		if !ok || from != "abc12345" || len(out) != len(payload) {
			b.Fatalf("decodeRawJSON failed: ok=%v from=%q len=%d", ok, from, len(out))
		}
	}
}
