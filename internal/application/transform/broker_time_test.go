package transform

import (
	"testing"
	"time"
)

// Regression för Orion-felet "Error parsing ISO8601 timestamp
// '...487585306Z' ... expected end-of-string": transformen sände
// nanosekunder (9 decimaler) som DateTime/observedAt, Orion avvisar mer än
// millisekunder (NGSv2) respektive mikrosekunder (LD, fel vid position 15).
// Broker-gränsen trunkeras därför alltid till millisekunder.
func TestBrokerTimeTruncatesNanosToMillis(t *testing.T) {
	nanos := time.Date(2026, 10, 22, 13, 16, 16, 487585306, time.UTC)
	if got := FormatBrokerTime(nanos); got != "2026-10-22T13:16:16.487Z" {
		t.Fatalf("FormatBrokerTime=%q", got)
	}
	// Exakt millisekund bevaras, nollor trimmas av RFC3339Nano.
	ms := time.Date(2026, 10, 22, 13, 16, 16, 487000000, time.UTC)
	if got := FormatBrokerTime(ms); got != "2026-10-22T13:16:16.487Z" {
		t.Fatalf("ms=%q", got)
	}
	// Hel sekund får ingen bråkdel.
	whole := time.Date(2026, 10, 22, 13, 16, 16, 0, time.UTC)
	if got := FormatBrokerTime(whole); got != "2026-10-22T13:16:16Z" {
		t.Fatalf("whole=%q", got)
	}
	if got := FormatBrokerTime(time.Time{}); got != "" {
		t.Fatalf("zero=%q", got)
	}
}

func TestBrokerTimestampNormalizationAcceptsNanosOutputsMillis(t *testing.T) {
	got, err := NormalizeBrokerTimestamp("2026-10-22T13:16:16.487585306Z")
	if err != nil || got != "2026-10-22T13:16:16.487Z" {
		t.Fatalf("normalize=%q err=%v", got, err)
	}
	if _, err := NormalizeBrokerTimestamp("not-a-date"); err == nil {
		t.Fatal("ogiltig tid accepterades")
	}
}

func TestBrokerValidationRejectsNanosAcceptsMillis(t *testing.T) {
	if err := validateTimestamp("2026-10-22T13:16:16.487585306Z"); err == nil {
		t.Fatal("nanosekunder ska nekas i broker-utdata")
	}
	for _, ok := range []string{
		"2026-10-22T13:16:16Z",
		"2026-10-22T13:16:16.487Z",
		"2026-10-22T13:16:16.48Z",
	} {
		if err := validateTimestamp(ok); err != nil {
			t.Fatalf("%s: %v", ok, err)
		}
	}
}

// WaterQualityObserved-fallet: dateObserved via timestamp:true från
// things.values med nanosekund-observedAt ska bli millisekunder.
func TestBrokerTimestampTrueTruncatesNanos(t *testing.T) {
	at := time.Date(2026, 10, 22, 13, 16, 16, 487585306, time.UTC)
	in := Thing{
		ID: "tank-1", Type: "tank", Event: EventThingsV1Values, ObservedAt: at,
		Fields: map[string]any{"geometry": map[string]any{"type": "Point", "coordinates": []any{17.0, 62.0}}},
	}
	r := Rule{
		ID:    "tank",
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "tank"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:WaterQualityObserved:tank-1", Type: "WaterQualityObserved",
			Properties: []Property{
				{Target: "location", Type: TypeGeo, Field: "geometry", Required: true},
				{Target: "dateObserved", Type: TypeDateTime, Timestamp: true, Required: true},
			},
		}},
	}
	out := stage5Output(t, in, r)
	date := out["dateObserved"].(map[string]any)["value"].(map[string]any)["@value"]
	if date != "2026-10-22T13:16:16.487Z" {
		t.Fatalf("dateObserved=%v", date)
	}
}
