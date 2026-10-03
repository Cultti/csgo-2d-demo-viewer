package parser

import (
	"strings"
	"time"

	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/events"
)

// ChatEntry timestamps are relative to the demo, not wall-clock time.
type ChatEntry struct {
	Tick        int     `json:"tick"`
	TimeSeconds float64 `json:"time_seconds"`
	SenderName  string  `json:"sender_name,omitempty"`
	Text        string  `json:"text"`
	MessageType string  `json:"message_type"`
}

func chatEntry(e events.SayText2, tick int, elapsed time.Duration) (ChatEntry, bool) {
	// Use raw SayText2: it retains the name even if the player cannot be resolved,
	// and includes team chat variants that ChatMessage does not dispatch.
	if !strings.HasPrefix(strings.TrimPrefix(e.MsgName, "#"), "Cstrike_Chat_") || len(e.Params) < 2 {
		return ChatEntry{}, false
	}
	return ChatEntry{
		Tick: tick, TimeSeconds: elapsed.Seconds(), SenderName: e.Params[0],
		Text: e.Params[1], MessageType: e.MsgName,
	}, true
}
