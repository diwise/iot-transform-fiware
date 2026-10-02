package transform

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
)

func TestStage6SupportedGeometryShapesAndImmutability(t *testing.T) {
	p := []any{17.0, 62.0}
	q := []any{18.0, 62.0}
	r := []any{18.0, 63.0}
	ring := []any{p, q, r, p}
	for kind, coordinates := range map[string]any{
		"Point": p, "MultiPoint": []any{p, q}, "LineString": []any{p, q}, "MultiLineString": []any{[]any{p, q}, []any{q, r}},
		"Polygon": []any{ring}, "MultiPolygon": []any{[]any{ring}, []any{ring}},
	} {
		t.Run(kind, func(t *testing.T) {
			raw := map[string]any{"type": kind, "coordinates": coordinates}
			before, _ := json.Marshal(raw)
			in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"geometry": raw}}
			out := stage5Output(t, in, stage5Rule(in, Property{Target: "customPosition", Type: TypeGeo, Field: "geometry"}))
			value := out["customPosition"].(map[string]any)["value"]
			if !reflect.DeepEqual(value, raw) {
				t.Fatalf("geometry serialization changed: %v %v", value, raw)
			}
			after, _ := json.Marshal(raw)
			if string(before) != string(after) {
				t.Fatal("geometry mutated input")
			}
		})
	}
}

func TestStage6EveryPropertyTypeHasProfileJSON(t *testing.T) {
	date := "2026-10-01T12:00:00.123456789Z"
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"clock": date}}
	rule := stage5Rule(in,
		Property{Target: "number", Type: TypeNumber, Const: 0, Unit: "CEL", ObservedBy: "urn:example:sensor"},
		Property{Target: "text", Type: TypeText, Const: "raw text"}, Property{Target: "boolean", Type: TypeBoolean, Const: false},
		Property{Target: "date", Type: TypeDateTime, Const: date}, Property{Target: "position", Type: TypeGeo, Const: map[string]any{"type": "Point", "coordinates": []any{17.0, 62.0, 9.0}}},
		Property{Target: "related", Type: TypeRelationship, Const: "urn:example:other"}, Property{Target: "relatedList", Type: TypeRelationship, Array: true, Const: "urn:example:other"},
		Property{Target: "numbers", Type: TypeNumberArray, Const: []any{0.0, 1.0}, Unit: "C62"}, Property{Target: "texts", Type: TypeTextArray, Const: []any{"one", "two"}},
		Property{Target: "localized", Type: TypeLanguageMap, Const: map[string]any{"en": "text", "@none": "fallback"}},
		Property{Target: "details", Type: TypeStructured, Subproperties: []Property{{Target: "temperature", Type: TypeNumber, Const: 0}, {Target: "valid", Type: TypeBoolean, Const: false}}},
	)
	for i := range rule.Entities[0].Properties {
		rule.Entities[0].Properties[i].ObservedAt = ObservedAt("field:clock")
	}
	out := stage5Output(t, in, rule)
	for _, p := range rule.Entities[0].Properties {
		attribute := out[p.Target].(map[string]any)
		if attribute["observedAt"] != date {
			t.Fatalf("%s time lost: %v", p.Type, attribute)
		}
		if p.Type == TypeRelationship {
			if attribute["type"] != "Relationship" || attribute["value"] != nil {
				t.Fatalf("relationship form: %v", attribute)
			}
		} else if p.Type == TypeGeo {
			if attribute["type"] != "GeoProperty" {
				t.Fatal(attribute)
			}
		} else if p.Type == TypeLanguageMap {
			if attribute["type"] != "LanguageProperty" || attribute["languageMap"] == nil {
				t.Fatal(attribute)
			}
		} else if attribute["type"] != "Property" {
			t.Fatal(attribute)
		}
	}
	if out["boolean"].(map[string]any)["value"] != false || out["number"].(map[string]any)["value"] != float64(0) {
		t.Fatal("zero/false lost")
	}
	if !reflect.DeepEqual(out["relatedList"].(map[string]any)["object"], []any{"urn:example:other"}) {
		t.Fatal("relationship list shape")
	}
	if !reflect.DeepEqual(out["date"].(map[string]any)["value"], map[string]any{"@type": "DateTime", "@value": date}) {
		t.Fatal("typed DateTime shape")
	}
}

func TestStage6MalformedSerializedMetadataAndNonfiniteNumbers(t *testing.T) {
	for _, value := range []any{math.NaN(), math.Inf(1), []any{1, math.Inf(-1)}, map[string]any{"nested": math.NaN()}} {
		write := EntityWrite{EntityID: "urn:example:object", TypeName: "Generic", Props: []entities.EntityDecoratorFunc{entities.P("reading", &genericProperty{Type_: "Property", Value_: value})}}
		if err := ValidateEntityWrites([]EntityWrite{write}, ""); err == nil {
			t.Fatalf("non-finite output accepted: %v", value)
		}
	}
	for _, attribute := range []map[string]any{
		{"type": "Property", "value": 1.0, "datasetId": "urn:example:dataset"},
		{"type": "Property", "value": true, "unitCode": "C62"},
		{"type": "Property", "value": []any{true, false}},
		{"type": "Property", "value": 1.0, "observedAt": "2026-10-01T12:00:00+24:00"},
		{"type": "Property", "value": map[string]any{"@type": "DateTime", "@value": "2026-02-30T12:00:00Z"}},
		{"type": "Property", "value": map[string]any{"@context": "https://example.com/nested"}},
		{"type": "Relationship", "object": []any{"urn:example:valid", false}},
		{"type": "LanguageProperty", "languageMap": map[string]any{"en_US": "text"}},
	} {
		if err := validateAttribute("reading", attribute); err == nil {
			t.Fatalf("unsupported output accepted: %v", attribute)
		}
	}
}

func TestStage6DynamicHeaderAndMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		change      func(*Entity)
	}{
		{"relative id", "relative-id", func(e *Entity) { e.ID = "{{candidate}}" }},
		{"malformed URI", "urn:example:bad%escape", func(e *Entity) { e.ID = "{{candidate}}" }},
		{"type whitespace", "bad type", func(e *Entity) { e.Type = "{{candidate}}" }},
		{"type keyword", "@id", func(e *Entity) { e.Type = "{{candidate}}" }},
		{"observedBy", "relative-observer", func(e *Entity) { e.Properties[0].ObservedBy = "{{candidate}}" }},
		{"attribute", "unused", func(e *Entity) { e.Properties[0].Target = "bad attribute" }},
		{"unit", "unused", func(e *Entity) { e.Properties[0].Unit = "bad unit" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"candidate": tc.value}}
			r := stage5Rule(in, Property{Target: "reading", Type: TypeNumber, Const: 1})
			tc.change(&r.Entities[0])
			_, err := NewEngine([]Rule{r}).Transform(in)
			if err == nil || !strings.Contains(err.Error(), "stage5") {
				t.Fatalf("invalid output accepted: %v", err)
			}
		})
	}
}

func TestStage6GeoJSONRejectsMalformedCoordinateStructures(t *testing.T) {
	for _, tc := range []struct {
		name, kind  string
		coordinates any
	}{
		{"scalar", "Point", 12}, {"one dimension", "Point", []any{17}},
		{"four dimensions", "Point", []any{17, 62, 0, 1}}, {"boolean", "Point", []any{true, 62}},
		{"numeric strings", "Point", []any{"17", "62"}}, {"longitude", "Point", []any{181, 62}},
		{"latitude", "Point", []any{17, 91}}, {"short line", "LineString", []any{[]any{17, 62}}},
		{"wrong line nesting", "LineString", []any{17, 62}},
		{"unclosed ring", "Polygon", []any{[]any{[]any{17, 62}, []any{18, 62}, []any{18, 63}, []any{17, 63}}}},
		{"short ring", "Polygon", []any{[]any{[]any{17, 62}, []any{18, 62}, []any{17, 62}}}},
		{"mixed dimensions", "MultiPoint", []any{[]any{17, 62}, []any{18, 63, 10}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"geometry": map[string]any{"type": tc.kind, "coordinates": tc.coordinates}}}
			r := stage5Rule(in, Property{Target: "area", Type: TypeGeo, Field: "geometry"})
			if err := Validate([]Rule{r}); err != nil {
				t.Fatalf("dynamic fixture must be valid: %v", err)
			}
			if _, err := NewEngine([]Rule{r}).Transform(in); err == nil {
				t.Fatal("malformed geometry accepted")
			}
		})
	}
}
