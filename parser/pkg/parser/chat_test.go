package parser

import (
	"testing"
	"time"

	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/events"
)

func TestChatEntry(t *testing.T) {
	for _, kind := range []string{"Cstrike_Chat_All", "Cstrike_Chat_AllDead", "Cstrike_Chat_CT_Loc", "#Cstrike_Chat_T_Dead"} {
		entry, ok := chatEntry(events.SayText2{MsgName: kind, Params: []string{"Player", "hello 世界", "Bombsite A"}}, 128, 2500*time.Millisecond)
		if !ok || entry.SenderName != "Player" || entry.Text != "hello 世界" || entry.Tick != 128 || entry.TimeSeconds != 2.5 || entry.MessageType != kind {
			t.Fatalf("unexpected entry for %s: %+v, %v", kind, entry, ok)
		}
	}
	entry, ok := chatEntry(events.SayText2{MsgName: "Cstrike_Chat_All", Params: []string{"", "hello"}}, 0, 0)
	if !ok || entry.SenderName != "" || entry.Text != "hello" {
		t.Fatalf("chat without sender name lost: %+v", entry)
	}
	for _, event := range []events.SayText2{
		{MsgName: "#Cstrike_Name_Change", Params: []string{"old", "new"}},
		{MsgName: "Cstrike_Chat_All", Params: []string{"Player"}},
	} {
		if _, ok := chatEntry(event, 0, 0); ok {
			t.Fatalf("accepted non-chat or malformed event: %+v", event)
		}
	}
}
