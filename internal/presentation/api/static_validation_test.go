package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func TestStaticValidationAtAPIBoundaries(t *testing.T) {
	h := &handler{}
	rule := testRule()
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		t.Fatal(err)
	}
	rule.Entities[0].Properties = []transform.Property{{Target: "sampled", Type: transform.TypeDateTime, Source: transform.Source{Const: "not-a-date"}}}
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
			if status != http.StatusBadRequest || !strings.Contains(body, "sampled") || !strings.Contains(body, "date time") {
				t.Fatalf("expected contextual validation error, got %d: %s", status, body)
			}
		})
	}
}

func TestStaticInvalidSnapshotKeepsWorkingConfiguration(t *testing.T) {
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
	invalid.Entities[0].Context = "relative-context"
	stored, err := store.InsertRule(t.Context(), invalid)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(t.Context()); err == nil || !strings.Contains(err.Error(), "entity.context") || !strings.Contains(err.Error(), stored.ID) {
		t.Fatalf("expected static validation failure, got %v", err)
	}
	if m.snap.Load() != before {
		t.Fatal("invalid static candidate replaced working snapshot")
	}
	if writes, err := m.Snapshot().Things.Transform(stubThing()); err != nil || len(writes) != 1 {
		t.Fatalf("working snapshot must still transform: %v, %v", writes, err)
	}
}
