package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func TestParityValidationAtAPIBoundaries(t *testing.T) {
	h := &handler{}
	rule := testRule()
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		t.Fatal(err)
	}
	rule.Entities[0].Properties = []transform.Property{{Target: "details", Type: transform.TypeStructured,
		Subproperties: []transform.Property{{Target: "reading", Type: transform.TypeNumber, Field: "reading", Unit: "CEL"}},
	}}
	for _, tt := range []struct {
		name string
		call func() (int, string)
	}{
		{"validate", func() (int, string) {
			rr := doModel(h.validateModel, http.MethodPost, "", rule, nil)
			return rr.Code, rr.Body.String()
		}},
		{"preview", func() (int, string) {
			rr := doModel(h.previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: json.RawMessage(`{}`)}}, nil)
			return rr.Code, rr.Body.String()
		}},
		{"create", func() (int, string) {
			rr := doModel(h.createModel, http.MethodPost, "", rule, nil)
			return rr.Code, rr.Body.String()
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, body := tt.call()
			if status != http.StatusBadRequest || !strings.Contains(body, "reading") || !strings.Contains(body, "metadata") {
				t.Fatalf("got %d: %s", status, body)
			}
		})
	}
}

func TestParityInvalidSnapshotKeepsWorkingConfiguration(t *testing.T) {
	store := &stubRuleStore{}
	if _, err := store.InsertRule(t.Context(), testRule()); err != nil {
		t.Fatal(err)
	}
	m := NewModels(store, nil)
	if err := m.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := m.snap.Load()
	invalid := testRule()
	invalid.Entities[0].Properties = []transform.Property{{Target: "details", Type: transform.TypeStructured,
		Subproperties: []transform.Property{{Target: "reading", Type: transform.TypeNumber, Field: "reading", Unit: "CEL"}},
	}}
	stored, err := store.InsertRule(t.Context(), invalid)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(t.Context()); err == nil || !strings.Contains(err.Error(), stored.ID) || !strings.Contains(err.Error(), "reading") {
		t.Fatalf("got %v", err)
	}
	if m.snap.Load() != before {
		t.Fatal("invalid candidate replaced working snapshot")
	}
	if writes, err := m.Snapshot().Things.Transform(stubThing()); err != nil || len(writes) != 1 {
		t.Fatalf("working snapshot lost: %v %v", writes, err)
	}
}

func TestParityPreviewSerializesSharedSources(t *testing.T) {
	h := &handler{}
	rule := transform.Rule{Match: transform.Match{Kind: transform.KindMeasurement}, Entities: []transform.Entity{{
		ID: "urn:example:{{id}}", Type: "Generic",
		Properties: []transform.Property{
			{Target: "total", Type: transform.TypeNumber, Derive: &transform.Derive{Fn: transform.FnSum, Of: []transform.Source{{Resource: "5700"}, {Const: 2}}}},
			{Target: "details", Type: transform.TypeStructured, Subproperties: []transform.Property{
				{Target: "reading", Type: transform.TypeNumber, Value: "{{5700}}"},
				{Target: "id", Type: transform.TypeText, Derive: &transform.Derive{Fn: transform.FnFirstPresent, Of: []transform.Source{{Value: "{{id}}"}}}},
			}},
		},
	}}}
	request := previewRequest{Rule: rule, Event: previewEvent{Kind: "measurement", Message: json.RawMessage(`{"pack":[{"bn":"sensor/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","v":12}],"timestamp":"2026-10-01T12:00:00Z"}`)}}
	rr := doModel(h.previewModel, http.MethodPost, "", request, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview: %d %s", rr.Code, rr.Body.String())
	}
	var out previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Matched || len(out.Entities) != 1 {
		t.Fatalf("unexpected response: %+v", out)
	}
	props := out.Entities[0].Properties
	if props["total"].(map[string]any)["value"] != float64(14) {
		t.Fatalf("aggregate: %+v", props)
	}
	details := props["details"].(map[string]any)["value"].(map[string]any)
	if details["reading"] != float64(12) || details["id"] != "sensor" {
		t.Fatalf("Structured: %+v", details)
	}
}
