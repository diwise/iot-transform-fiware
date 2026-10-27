package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
)

func TestAggregateRuleValidationAndHonestInlinePreview(t *testing.T) {
	rules, err := config.LoadModelsDir("../../../deployments/examples/tank-greenhouse")
	if err != nil {
		t.Fatal(err)
	}
	h := &handler{}
	r := rules[0]
	validated := doModel(h.validateModel, http.MethodPost, "", r, nil)
	if validated.Code != http.StatusOK {
		t.Fatal(validated.Code, validated.Body.String())
	}
	preview := doModel(h.previewModel, http.MethodPost, "", map[string]any{"rule": r, "event": map[string]any{"kind": "things.v1.relations", "message": map[string]any{"tenant": "example", "fromId": "tank-1", "toId": "greenhouse-1", "type": "partOf"}}}, nil)
	if preview.Code != http.StatusBadRequest || !strings.Contains(preview.Body.String(), "complete durable projection") {
		t.Fatal(preview.Code, preview.Body.String())
	}
	r.Entities[1].Properties[0].Aggregate.Direction = "unknown"
	invalid := doModel(h.validateModel, http.MethodPost, "", r, nil)
	if invalid.Code != http.StatusBadRequest {
		t.Fatal(invalid.Code, invalid.Body.String())
	}
}
