package transform

import (
	"testing"
)

// Händelsekällan skiljer gamla och nya regler åt: tom tagg matchar endast
// omärkt indata (ursprungliga källor), satt tagg endast samma tagg.
func TestEventTagIsolatesOldAndNewRules(t *testing.T) {
	old := Thing{ID: "r", Type: "Room", TypeName: "Room", NameOrID: "r", Tenant: "t"}
	v1 := old
	v1.Event = EventThingsV1Values

	oldRule := Rule{Match: Match{Kind: KindThing, Type: "Room"}}
	newRule := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "Room"}}

	if !oldRule.Match.matches(old, old.MatchInfo()) {
		t.Fatal("gammal regel ska matcha omärkt indata")
	}
	if oldRule.Match.matches(v1, v1.MatchInfo()) {
		t.Fatal("gammal regel får inte aktiveras av versionerat event")
	}
	if !newRule.Match.matches(v1, v1.MatchInfo()) {
		t.Fatal("ny regel ska matcha versionerat event")
	}
	if newRule.Match.matches(old, old.MatchInfo()) {
		t.Fatal("ny regel får inte aktiveras av ursprunglig källa")
	}
}

func TestEventTagValidation(t *testing.T) {
	ok := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "room"},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X"}}}
	if err := ok.validate(); err != nil {
		t.Fatalf("känd event-tagg ska passera: %v", err)
	}
	bad := Rule{Match: Match{Kind: KindThing, Event: "nope", Type: "room"}}
	if err := bad.validate(); err == nil {
		t.Fatal("okänd event-tagg ska avvisas (typo-skydd)")
	}
	meas := Rule{Match: Match{Kind: KindMeasurement, Event: EventThingsV1Values}}
	if err := meas.validate(); err == nil {
		t.Fatal("event-tagg på mätvärdesregel ska avvisas")
	}
}
