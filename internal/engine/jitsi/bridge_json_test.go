package jitsi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/zarazaex69/j"
)

func TestEndpointMessageJSONWireCompatibility(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		to      string
		payload []byte
	}{
		{name: "empty", to: "abc12345", payload: nil},
		{name: "broadcast", to: "", payload: []byte("hello")},
		{name: "12kib", to: "deadbeef", payload: make([]byte, 12*1024)},
		{name: "special-to", to: `a"b\c<d>e&f🚀`, payload: []byte{0, 1, 2, 255}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(newEndpointMessage(tc.to, tc.payload))
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			type wirePayload struct {
				Raw string `json:"raw"`
			}
			type wireMsg struct {
				ColibriClass string      `json:"colibriClass"` //nolint:tagliatelle // JVB wire protocol
				To           string      `json:"to"`
				MsgPayload   wirePayload `json:"msgPayload"` //nolint:tagliatelle // JVB wire protocol
			}
			want, err := json.Marshal(wireMsg{
				ColibriClass: colibriClassEndpointMessage,
				To:           tc.to,
				MsgPayload:   wirePayload{Raw: base64.StdEncoding.EncodeToString(tc.payload)},
			})
			if err != nil {
				t.Fatalf("want Marshal() error = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Marshal() = %s, want %s", got, want)
			}
		})
	}
}

func TestDecodeRawAcceptsCurrentAndLegacy(t *testing.T) {
	t.Parallel()
	raw := []byte("frame-data")
	enc := base64.StdEncoding.EncodeToString(raw)

	current := j.BridgeMessage{
		Class:  colibriClassEndpointMessage,
		Fields: map[string]any{"msgPayload": map[string]any{"raw": enc}},
	}
	if got := decodeRaw(current); !bytes.Equal(got, raw) {
		t.Fatalf("current decodeRaw = %q, want %q", got, raw)
	}

	legacy := j.BridgeMessage{
		Class:  colibriClassEndpointMessage,
		Fields: map[string]any{"raw": enc},
	}
	if got := decodeRaw(legacy); !bytes.Equal(got, raw) {
		t.Fatalf("legacy decodeRaw = %q, want %q", got, raw)
	}

	bad := j.BridgeMessage{
		Class:  colibriClassEndpointMessage,
		Fields: map[string]any{"msgPayload": map[string]any{"raw": "%%%not-base64%%%"}},
	}
	if got := decodeRaw(bad); got != nil {
		t.Fatalf("invalid decodeRaw = %q, want nil", got)
	}
}

func TestDecodeRawJSON(t *testing.T) {
	t.Parallel()
	raw := []byte("hello-fast-path")
	enc := base64.StdEncoding.EncodeToString(raw)

	from, got, ok := decodeRawJSON([]byte(`{"colibriClass":"EndpointMessage","from":"peer-1","to":"me","msgPayload":{"raw":"` + enc + `"}}`))
	if !ok || from != "peer-1" || !bytes.Equal(got, raw) {
		t.Fatalf("msgPayload decodeRawJSON = (%q, %q, %v), want (%q, %q, true)", from, got, ok, "peer-1", raw)
	}

	from, got, ok = decodeRawJSON([]byte(`{ "from" : "peer-2" , "raw" : "` + enc + `" , "colibriClass" : "EndpointMessage" }`))
	if !ok || from != "peer-2" || !bytes.Equal(got, raw) {
		t.Fatalf("legacy decodeRawJSON = (%q, %q, %v), want (%q, %q, true)", from, got, ok, "peer-2", raw)
	}

	from, got, ok = decodeRawJSON([]byte(`{"colibriClass":"EndpointMessage","from":"a\"b\\c","msgPayload":{"raw":"` + enc + `"}}`))
	if !ok || from != `a"b\c` || !bytes.Equal(got, raw) {
		t.Fatalf("escaped from decodeRawJSON = (%q, %q, %v), want (%q, %q, true)", from, got, ok, `a"b\c`, raw)
	}

	for _, nonData := range [][]byte{
		[]byte(`{"colibriClass":"ServerHello"}`),
		[]byte(`{"colibriClass":"DominantSpeakerEndpointChangeEvent","dominantSpeakerEndpoint":"p1"}`),
		[]byte(`not-json`),
		[]byte(`{"colibriClass":"EndpointMessage","from":"p1","msgPayload":{"raw":"%%%bad%%%"}}`),
		[]byte(`{"colibriClass":"EndpointMessage","from":"p1","msgPayload":{"other":1}}`),
	} {
		if _, _, ok := decodeRawJSON(nonData); ok {
			t.Fatalf("decodeRawJSON(%s) = ok, want false", nonData)
		}
	}
}
