package client

import "github.com/openlibrecommunity/olcrtc/internal/logger"

// RoomState describes room entry and relay session establishment separately.
// ai-generated: the room state API and values.
type RoomState string

const (
	RoomJoined     RoomState = "joined"
	RoomJoinFailed RoomState = "join-failed"
	RelayReady     RoomState = "relay-ready"
	RelayFailed    RoomState = "relay-failed"
)

// notifyRoomState reports a state without exposing the room or key in the log.
// ai-generated: structured room state notification.
func (c *Client) notifyRoomState(state RoomState) {
	logger.Infof("client: room state=%s", state)
	if c.onRoomState != nil {
		c.onRoomState(state)
	}
}
