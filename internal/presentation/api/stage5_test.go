package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func TestStage5PreviewPreservesOwnTimeConversionAndEmptySkip(t *testing.T) {
	rule := transform.Rule{Priority: 3, Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values}, Entities: []transform.Entity{{ID: "urn:example:{{id}}", Type: "Generic", Properties: []transform.Property{{Target: "reading", Type: transform.TypeNumber, Field: "values.reading.value", Convert: &transform.Conversion{From: "Cel", To: "KEL", Offset: 273.15}}}}}}
	for _, value := range []string{"0", "null"} {
		body := `{"thingId":"object","tenant":"tenant-a","thingType":"source","publishedAt":"2026-10-01T13:00:00Z","values":[{"propertyId":"reading","value":` + value + `,"quality":"ok","unit":"Cel","observedAt":"2026-10-01T12:00:00.123456789Z"},{"propertyId":"other","value":99,"quality":"invalid","observedAt":"2026-10-01T12:01:00Z"}]}`
		rr := doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: json.RawMessage(body)}}, nil)
		if rr.Code != http.StatusOK {
			t.Fatalf("preview %d %s", rr.Code, rr.Body.String())
		}
		var out previewResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if !out.Matched {
			t.Fatal("matched skip lost its match")
		}
		if value == "null" {
			if len(out.Entities) != 0 {
				t.Fatalf("null wrote: %+v", out)
			}
			continue
		}
		if len(out.Entities) != 1 {
			t.Fatalf("writes: %+v", out)
		}
		entry := out.Entities[0].Properties["reading"].(map[string]any)
		if entry["value"] != 273.15 || entry["unitCode"] != "KEL" || entry["observedAt"] != "2026-10-01T12:00:00.123Z" {
			t.Fatalf("preview changed semantics: %+v", entry)
		}
	}
}

func TestStage5DatabaseSnapshotPreservesNewRuleFields(t *testing.T) {
	m := pgTestModels(t)
	rule := testRule()
	rule.Priority = 7
	rule.Entities[0].Properties = []transform.Property{{Target: "reading", Type: transform.TypeNumber, Const: 12.0, Convert: &transform.Conversion{From: "Cel", To: "KEL", Offset: 273.15}}}
	stored, err := m.store.InsertRule(t.Context(), rule)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := m.store.GetRule(t.Context(), stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Rule.Priority != 7 || loaded.Rule.Entities[0].Properties[0].Convert.Offset != 273.15 {
		t.Fatalf("DB lost fields: %+v", loaded)
	}
	writes, err := m.Snapshot().Things.Transform(stubThing())
	if err != nil || len(writes) != 1 || writes[0].RuleID != stored.ID {
		t.Fatalf("snapshot: %v %v", writes, err)
	}
	rule.Match.Event, rule.Match.Type, rule.Match.Relation, rule.Match.ToType = transform.EventThingsV1Relations, "source", "partOf", "target"
	stored, err = m.store.InsertRule(t.Context(), rule)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err = m.store.GetRule(t.Context(), stored.ID)
	if err != nil || loaded.Rule.Match.ToType != "target" {
		t.Fatalf("DB lost target filter: %+v %v", loaded, err)
	}
}
