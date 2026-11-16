package client

import internalclient "github.com/openlibrecommunity/olcrtc/internal/client"

// RoomState describes room entry and relay session establishment separately.
// ai-generated: public aliases for the internal room state API.
type RoomState = internalclient.RoomState

const (
	RoomJoined     = internalclient.RoomJoined
	RoomJoinFailed = internalclient.RoomJoinFailed
	RelayReady     = internalclient.RelayReady
	RelayFailed    = internalclient.RelayFailed
)
