package things

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// Wire fields mirror iot-things-v2/pkg/types, without importing its Go module.
const sourceValues = `{"schema":"v1","eventId":"event-1","tenant":"tenant-a","thingId":"object-1","thingType":"source-template","thingTypeVersion":"2","configRevision":7,"publishedAt":"2026-10-01T12:03:00.123456789Z","thing":{"thingId":"object-1","name":"Raw name å","alternativeName":"Raw alias","subType":"raw-subtype","category":"source-category","variantId":"variant-a","variantVersion":"3","metadata":{"description":"Source description"},"location":{"type":"Point","coordinates":[17,62,9]}},"values":[{"schema":"v1","eventId":"observation-1","thingId":"object-1","tenant":"tenant-a","propertyId":"reading.average","value":0,"quality":"uncertain","unit":"Cel","source":"sensor/2/3303/5700","messageId":"message-1","observedAt":"2026-10-01T12:00:00.123456789Z","receivedAt":"2026-10-01T12:01:00.234567891Z","computedAt":"2026-10-01T12:02:00.345678912Z","publishedAt":"2026-10-01T12:03:00.456789123Z","configRevision":7},{"propertyId":"invalid","value":99,"quality":"invalid","source":"sensor/2/3304/5700","messageId":"message-invalid","observedAt":"2026-10-01T12:00:01Z"},{"propertyId":"null","value":null,"quality":"ok"}]}`

func assertSourceField(t *testing.T, input transform.Thing, path string, want any) {
	t.Helper()
	got, ok := input.ResolvePath(path)
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %#v (present=%t), want %#v", path, got, ok, want)
	}
}

func sourceEntityJSON(t *testing.T, writes []transform.EntityWrite) map[string]any {
	t.Helper()
	if len(writes) != 1 {
		t.Fatalf("expected one write, got %v", writes)
	}
	entity, err := entities.New(writes[0].EntityID, writes[0].TypeName, writes[0].Props...)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	if err := json.Unmarshal(raw, &output); err != nil {
		t.Fatal(err)
	}
	return output
}

func TestSourceContractQualityCannotBeBypassedThroughMetadata(t *testing.T) {
	for _, quality := range []string{"ok", "uncertain", "invalid", "unknown", ""} {
		t.Run(quality, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal([]byte(sourceValues), &body); err != nil {
				t.Fatal(err)
			}
			body["values"].([]any)[0].(map[string]any)["quality"] = quality
			raw, _ := json.Marshal(body)
			input, err := ThingValuesFromJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			rule := transform.Rule{Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values}, Entities: []transform.Entity{{ID: "urn:example:{{id}}", Type: "Generic", Properties: []transform.Property{
				{Target: "reading", Type: transform.TypeNumber, Field: `values."reading.average".value`, Required: true},
			}}}}
			if err := transform.Validate([]transform.Rule{rule}); err != nil {
				t.Fatal(err)
			}
			writes, err := NewDynamicThingTransformer([]transform.Rule{rule}).Transform(input)
			if err != nil {
				t.Fatal(err)
			}
			if quality == "ok" || quality == "uncertain" {
				output := sourceEntityJSON(t, writes)
				if output["reading"].(map[string]any)["value"] != float64(0) {
					t.Fatalf("zero lost: %v", output)
				}
			} else if len(writes) != 0 {
				t.Fatalf("invalid reading writes: %v", writes)
			}
			for _, path := range []string{"observations[0].value", "event.values[0].value"} {
				rule.Entities[0].Properties[0].Field = path
				writes, err = NewDynamicThingTransformer([]transform.Rule{rule}).Transform(input)
				if err != nil || len(writes) != 0 {
					t.Fatalf("metadata bypass %s: %v %v", path, writes, err)
				}
			}
			metadata, _ := json.Marshal(input.Fields["observations"])
			if strings.Contains(string(metadata), `"value":`) {
				t.Fatal("observations view leaked numeric source values")
			}
		})
	}
}

func TestSourceContractLifecycleFieldsProduceGenericEntity(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal([]byte(sourceValues), &body); err != nil {
		t.Fatal(err)
	}
	body["kind"], body["changedAt"] = "updated", "2026-10-01T12:03:00.123456789Z"
	raw, _ := json.Marshal(body)
	input, err := ThingLifecycleFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	rule := transform.Rule{Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Lifecycle, Lifecycle: "updated"}, Entities: []transform.Entity{{ID: "urn:example:{{id}}", Type: "Generic", Properties: []transform.Property{
		{Target: "description", Type: transform.TypeText, Field: "metadata.description", ObservedAt: transform.ObservedAtNone},
		{Target: "position", Type: transform.TypeGeo, Field: "geometry", ObservedAt: transform.ObservedAt("field:event.changedAt")},
		{Target: "variant", Type: transform.TypeText, Value: "{{thing.variantId}}:{{thing.variantVersion}}", ObservedAt: transform.ObservedAtNone},
	}}}}
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		t.Fatal(err)
	}
	writes, err := NewDynamicThingTransformer([]transform.Rule{rule}).Transform(input)
	if err != nil {
		t.Fatal(err)
	}
	output := sourceEntityJSON(t, writes)
	if output["description"].(map[string]any)["value"] != "Source description" || output["variant"].(map[string]any)["value"] != "variant-a:3" {
		t.Fatalf("metadata missing: %v", output)
	}
	coords := output["position"].(map[string]any)["value"].(map[string]any)["coordinates"].([]any)
	if len(coords) != 3 || coords[2] != float64(9) {
		t.Fatalf("raw geometry truncated: %v", coords)
	}
}

func TestSourceContractValuesPreservesRelatedChangesAndMissingTimes(t *testing.T) {
	const raw = `{"thingId":"object-1","thingType":"source-template","tenant":"tenant-a","values":[],"relations":[{"relationId":"relation-1","type":"source-relation","fromId":"object-1","toId":"object-2","removed":true,"tenant":"tenant-a","revision":4,"changedAt":"2026-10-01T12:00:00.123456789Z","fromThingType":"source-template","toThingType":"other-template","from":{"thingId":"object-1"},"to":{"thingId":"object-2","metadata":{"description":"Other source"}}}]}`
	input, err := ThingValuesFromJSON([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]any{
		"relations[0].relationId": "relation-1", "relations[0].removed": true, "relations[0].toId": "object-2",
		"relations[0].tenant": "tenant-a", "relations[0].to.type": "other-template", "relations[0].to.metadata.description": "Other source",
		"relations[0].changedAt": "2026-10-01T12:00:00.123456789Z",
	} {
		assertSourceField(t, input, path, want)
	}
	if !input.ObservedAt.IsZero() {
		t.Fatal("relation time must not become measurement time")
	}
	if _, ok := input.ResolvePath("event.publishedAt"); ok {
		t.Fatal("missing publish time fabricated")
	}
	lifecycle, err := ThingLifecycleFromJSON([]byte(`{"thingId":"object-1","thingType":"source-template","tenant":"tenant-a","kind":"deleted"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"event.changedAt", "lifecycle.changedAt", "geometry", "thing.location", "metadata"} {
		if _, ok := lifecycle.ResolvePath(path); ok {
			t.Fatalf("missing source fabricated: %s", path)
		}
	}
}

func TestSourceContractValuesPreservesEnvelopeAndProvenance(t *testing.T) {
	in, err := ThingValuesFromJSON([]byte(sourceValues))
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]any{
		"event.schema": "v1", "event.eventId": "event-1", "event.tenant": "tenant-a", "event.thingId": "object-1",
		"event.thingType": "source-template", "event.thingTypeVersion": "2", "event.configRevision": int64(7),
		"event.publishedAt":               "2026-10-01T12:03:00.123456789Z",
		`values."reading.average".schema`: "v1", `values."reading.average".eventId`: "observation-1",
		`values."reading.average".thingId`: "object-1", `values."reading.average".tenant`: "tenant-a",
		`values."reading.average".propertyId`: "reading.average", `values."reading.average".value`: float64(0),
		`values."reading.average".messageId`: "message-1", `values."reading.average".configRevision`: int64(7),
		`values."reading.average".observedAt`:  "2026-10-01T12:00:00.123456789Z",
		`values."reading.average".receivedAt`:  "2026-10-01T12:01:00.234567891Z",
		`values."reading.average".computedAt`:  "2026-10-01T12:02:00.345678912Z",
		`values."reading.average".publishedAt`: "2026-10-01T12:03:00.456789123Z",
		"observations[1].quality":              "invalid", "observations[1].messageId": "message-invalid",
		"observations[1].valuePresent": true, "observations[2].valuePresent": false,
	} {
		assertSourceField(t, in, path, want)
	}
	for _, path := range []string{"values.invalid.value", "values.null.value", "observations[1].value", "event.values[1].value", "observations[2].observedAt", "observations[2].unit"} {
		if value, ok := in.ResolvePath(path); ok {
			t.Fatalf("unsafe/invented source %s: %v", path, value)
		}
	}
}

func TestSourceContractDescriptorsAndGeometryConsistent(t *testing.T) {
	var values map[string]any
	if err := json.Unmarshal([]byte(sourceValues), &values); err != nil {
		t.Fatal(err)
	}
	for _, geometry := range []map[string]any{
		{"type": "Point", "coordinates": []any{float64(17), float64(62), float64(9)}},
		{"type": "Polygon", "coordinates": []any{[]any{[]any{float64(17), float64(62)}, []any{float64(18), float64(62)}, []any{float64(18), float64(63)}, []any{float64(17), float64(62)}}}},
	} {
		values["thing"].(map[string]any)["location"] = geometry
		for _, kind := range []string{"values", "lifecycle", "relations"} {
			t.Run(geometry["type"].(string)+"/"+kind, func(t *testing.T) {
				values["kind"], values["changedAt"] = "updated", "2026-10-01T12:03:00.123456789Z"
				values["from"], values["to"] = values["thing"], values["thing"]
				values["fromId"], values["toId"], values["fromThingType"], values["toThingType"], values["type"] = "object-1", "object-1", "source-template", "source-template", "source-relation"
				raw, _ := json.Marshal(values)
				in, err := ThingValuesFromJSON(raw)
				if kind == "lifecycle" {
					in, err = ThingLifecycleFromJSON(raw)
				}
				if kind == "relations" {
					in, err = ThingRelationsFromJSON(raw)
				}
				if err != nil {
					t.Fatal(err)
				}
				for path, want := range map[string]any{"geometry": geometry, "metadata.description": "Source description", "category": "source-category", "variantId": "variant-a", "variantVersion": "3"} {
					assertSourceField(t, in, path, want)
				}
				if in.ID != "object-1" || in.Type != "source-template" || in.TypeName != "raw-subtype" || in.Name != "Raw name å" {
					t.Fatalf("raw identity changed: %+v", in)
				}
				paths := []string{"thing"}
				if kind == "relations" {
					paths = []string{"relation.from", "relation.to"}
				}
				for _, path := range paths {
					assertSourceField(t, in, path+".location", geometry)
				}
				_, _, point := in.LatLon()
				if point != (geometry["type"] == "Point") {
					t.Fatalf("incorrect coordinate shortcut for %s", geometry["type"])
				}
				if kind == "lifecycle" {
					assertSourceField(t, in, "event.schema", "v1")
					assertSourceField(t, in, "event.thingTypeVersion", "2")
					assertSourceField(t, in, "event.changedAt", "2026-10-01T12:03:00.123456789Z")
				}
			})
		}
	}
}
