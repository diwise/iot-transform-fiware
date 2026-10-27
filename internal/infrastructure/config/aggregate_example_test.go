package config

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func TestTankGreenhouseAggregateExample(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/examples/tank-greenhouse")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../../deployments/examples/tank-greenhouse/links.json")
	if err != nil {
		t.Fatal(err)
	}
	links, err := relations.LinksFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	engine := transform.NewEngine(rules)
	writes, err := engine.RelationPlanner(nil, "")(relations.Snapshot{Tenant: "example", Reconcile: true,
		Groups: relations.SnapshotsForReplacement(nil, links, "example")})
	if err != nil {
		t.Fatal(err)
	}
	inverseCount, forwardCount := 0, 0
	for _, w := range writes {
		var doc map[string]any
		if err := json.Unmarshal(w.Payload, &doc); err != nil {
			t.Fatal(err)
		}
		if ref, ok := doc["refParent"].(map[string]any); ok {
			forwardCount++
			if ref["object"] != "urn:ngsi-ld:AgriGreenhouse:greenhouse-1" {
				t.Fatal(ref)
			}
		}
		if rel, ok := doc["hasWaterQualityObserved"].(map[string]any); ok {
			inverseCount++
			want := []any{"urn:ngsi-ld:WaterQualityObserved:tank-1", "urn:ngsi-ld:WaterQualityObserved:tank-2", "urn:ngsi-ld:WaterQualityObserved:tank-3", "urn:ngsi-ld:WaterQualityObserved:tank-4"}
			if rel["type"] != "Relationship" || !reflect.DeepEqual(rel["object"], want) {
				t.Fatalf("unexpected inverse: %s", w.Payload)
			}
		}
	}
	if inverseCount != 1 || forwardCount != 4 {
		t.Fatalf("inverse=%d forward=%d", inverseCount, forwardCount)
	}
	// Neither descriptive updates nor measurements touch the owned relation.
	for _, in := range []transform.Thing{
		{ID: "greenhouse-1", Type: "greenhouse", Tenant: "example", Event: transform.EventThingsV1Lifecycle, Lifecycle: "updated", Name: "Renamed"},
		{ID: "greenhouse-1", Type: "greenhouse", Tenant: "example", Event: transform.EventThingsV1Values, Fields: map[string]any{"values": map[string]any{"co2": map[string]any{"value": 450.0}}}},
	} {
		out, err := engine.Transform(in)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := transform.EntityWritesToPending("example", out, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range pending {
			var doc map[string]any
			if err := json.Unmarshal(p.Payload, &doc); err != nil {
				t.Fatal(err)
			}
			if _, ok := doc["hasWaterQualityObserved"]; ok {
				t.Fatal("unrelated update overwrote aggregate")
			}
		}
	}
}
