package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func TestSourceContractPreviewMeasurementPartitionsLikeProduction(t *testing.T) {
	const raw = `{"timestamp":"2026-10-01T12:04:00Z","pack":[
 {"bn":"sensor/2/3303/","bt":1790856000.25,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
 {"n":"5700","v":0,"u":"Cel","t":0.5},{"n":"env","vs":"first"},{"n":"lat","v":0,"u":"lat"},{"n":"lon","v":17,"u":"lon"},
 {"bn":"sensor/2/3303/","bt":1790856001.25,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
 {"n":"5700","vb":false,"u":"1","t":0.75},{"n":"env","vs":"second"},{"n":"lat","v":62,"u":"lat"},{"n":"lon","v":18,"u":"lon"},
 {"bn":"sensor/","n":"tenant","vs":"tenant-a"},{"n":"env","vs":"default"}]}`
	rule := transform.Rule{Match: transform.Match{Kind: transform.KindMeasurement}, Entities: []transform.Entity{{ID: "urn:example:{{id}}:{{env}}", Type: "Generic", Properties: []transform.Property{
		{Target: "reading", Type: transform.TypeText, Value: "{{records[0].value}}", ObservedAt: transform.ObservedAtNone},
		{Target: "unit", Type: transform.TypeText, Value: "{{records[0].unit}}", ObservedAt: transform.ObservedAtNone},
		{Target: "sourceIdentity", Type: transform.TypeText, Value: "{{records[0].fullName}}", ObservedAt: transform.ObservedAtNone},
		{Target: "position", Type: transform.TypeGeo, LatLon: true},
		{Target: "sampled", Type: transform.TypeDateTime, Value: "{{records[0].observedAt}}"},
	}}}}
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		t.Fatal(err)
	}
	rr := doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: "measurement", Message: json.RawMessage(raw)}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview %d: %s", rr.Code, rr.Body.String())
	}
	var out previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Observations != 2 || len(out.Entities) != 2 {
		t.Fatalf("wrong partition count: %+v", out)
	}
	var message events.MessageAccepted
	if err := json.Unmarshal([]byte(raw), &message); err != nil {
		t.Fatal(err)
	}
	observations, err := measurements.PartitionMessage(message, message.Timestamp)
	if err != nil {
		t.Fatal(err)
	}
	production := measurements.NewDynamicTransformer([]transform.Rule{rule}, nil)
	for i, observation := range observations {
		writes, err := production.Transform(t.Context(), observation)
		if err != nil || len(writes) != 1 {
			t.Fatalf("production %v %v", writes, err)
		}
		properties, err := entityProperties(writes[0].EntityID, writes[0].TypeName, writes[0].Props)
		if err != nil {
			t.Fatal(err)
		}
		if writes[0].EntityID != out.Entities[i].ID || !reflect.DeepEqual(properties, out.Entities[i].Properties) {
			t.Fatalf("preview/production differ: %v %v", properties, out.Entities[i])
		}
		if out.Entities[i].ID != "urn:example:sensor:"+[]string{"first", "second"}[i] {
			t.Fatalf("metadata crossed observations: %+v", out.Entities[i])
		}
		if properties["reading"].(map[string]any)["value"] != []string{"0", "false"}[i] || properties["unit"].(map[string]any)["value"] != []string{"Cel", "1"}[i] {
			t.Fatalf("wrong source record: %v", properties)
		}
		if properties["sourceIdentity"].(map[string]any)["value"] != "sensor/2/3303/5700" {
			t.Fatalf("identity lost: %v", properties)
		}
	}
}

func TestSourceContractPreviewLifecycleUsesRawDescriptor(t *testing.T) {
	rule := transform.Rule{Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Lifecycle, Lifecycle: "updated"}, Entities: []transform.Entity{{ID: "urn:example:{{id}}", Type: "Generic", Properties: []transform.Property{
		{Target: "description", Type: transform.TypeText, Field: "metadata.description", Required: true, ObservedAt: transform.ObservedAtNone},
		{Target: "area", Type: transform.TypeGeo, Field: "geometry", Required: true},
	}}}}
	const raw = `{"schema":"v1","eventId":"event-1","tenant":"tenant-a","thingId":"object-1","kind":"updated","thingType":"source-template","thingTypeVersion":"2","configRevision":7,"changedAt":"2026-10-01T12:03:00.123456789Z","thing":{"thingId":"object-1","metadata":{"description":"Source description"},"location":{"type":"Polygon","coordinates":[[[17,62],[18,62],[18,63],[17,62]]]}}}`
	rr := doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Lifecycle, Message: json.RawMessage(raw)}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview %d: %s", rr.Code, rr.Body.String())
	}
	var out previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Matched || len(out.Entities) != 1 || out.Entities[0].Properties["description"].(map[string]any)["value"] != "Source description" {
		t.Fatalf("descriptor unavailable: %+v", out)
	}
	if out.Entities[0].Properties["area"].(map[string]any)["value"].(map[string]any)["type"] != "Polygon" {
		t.Fatalf("geometry lost: %+v", out)
	}
}
