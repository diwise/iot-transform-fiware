package api

import (
	"encoding/json"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func TestStage6PreviewAndProductionRejectSameOutput(t *testing.T) {
	const raw = `{"thingId":"object","thingType":"source","tenant":"tenant-a","thing":{"thingId":"object","location":{"type":"Polygon","coordinates":[[[17,62],[18,62],[18,63],[17,63]]]}}}`
	in, err := things.ThingValuesFromJSON([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []transform.Rule{
		{Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values}, Entities: []transform.Entity{{ID: "{{id}}", Type: "Generic", Properties: []transform.Property{{Target: "reading", Type: transform.TypeNumber, Const: 1}}}}},
		{Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values}, Entities: []transform.Entity{{ID: "urn:example:{{id}}", Type: "Generic", Properties: []transform.Property{{Target: "position", Type: transform.TypeGeo, Field: "geometry"}}}}},
	} {
		if err := transform.Validate([]transform.Rule{rule}); err != nil {
			t.Fatalf("valid dynamic rule: %v", err)
		}
		_, productionErr := things.NewDynamicThingTransformer([]transform.Rule{rule}).Transform(in)
		_, _, _, previewErr := previewTransform(t.Context(), previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: json.RawMessage(raw)}})
		if productionErr == nil || previewErr == nil || productionErr.Error() != previewErr.Error() {
			t.Fatalf("validator differs: production=%v preview=%v", productionErr, previewErr)
		}
	}
}
