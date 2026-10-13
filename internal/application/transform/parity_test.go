package transform

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func parityRule(in Input, p Property) Rule {
	r := Rule{ID: "parity-rule", Match: Match{Kind: in.Kind()}, Entities: []Entity{{
		ID: "urn:example:{{id}}", Type: "Generic", Properties: []Property{p},
	}}}
	if in.Kind() == KindThing {
		r.Match.Event = EventThingsV1Values
	}
	return r
}

func TestExecutionParitySources(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	m := Measurement{DeviceID: "sensor", Timestamp: now, HasLatLon: true, Lat: 62, Lon: 17,
		Records: []Record{{Name: "reading", String: new("12")}}}
	th := Thing{ID: "object", Event: EventThingsV1Values, ObservedAt: now,
		Fields: map[string]any{"reading": "12", "location": map[string]any{"latitude": 62, "longitude": 17}}}
	cases := []struct {
		name   string
		in     Input
		source Source
		typ    PropertyType
		want   any
	}{
		{"resource", m, Source{Resource: "reading"}, TypeNumber, float64(12)},
		{"name", m, Source{Name: "reading"}, TypeNumber, float64(12)},
		{"field", th, Source{Field: "reading"}, TypeNumber, float64(12)},
		{"ref", th, Source{Ref: "reading"}, TypeNumber, float64(12)},
		{"const", m, Source{Const: "12"}, TypeNumber, float64(12)},
		{"false", th, Source{Const: false}, TypeBoolean, false},
		{"zero", m, Source{Const: 0}, TypeNumber, float64(0)},
		{"text", th, Source{Const: 12}, TypeText, "12"},
		{"number array", th, Source{Const: []any{"1", 2}}, TypeNumberArray, []any{float64(1), float64(2)}},
		{"text array", m, Source{Const: []any{1, true}}, TypeTextArray, []any{"1", "true"}},
		{"value", th, Source{Value: "{{reading}}"}, TypeNumber, float64(12)},
		{"prefix", th, Source{Field: "reading", Prefix: "urn:example:"}, TypeRelationship, "urn:example:12"},
		{"timestamp", m, Source{Timestamp: true}, TypeDateTime, now.Format(time.RFC3339)},
		{"latlon", m, Source{LatLon: true}, TypeGeo, map[string]any{"type": "Point", "coordinates": []any{float64(17), float64(62)}}},
	}
	for _, tc := range cases {
		for _, mode := range []string{"property", "derive", "structured", "structured derive"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				p := Property{Target: "result", Type: tc.typ, Source: tc.source, ObservedAt: ObservedAtNone}
				if tc.source.Timestamp || tc.typ == TypeDateTime {
					p.ObservedAt = ""
				}
				if strings.Contains(mode, "derive") {
					p.Source = Source{}
					p.Derive = &Derive{Fn: FnFirstPresent, Of: []Source{tc.source}}
				}
				if strings.HasPrefix(mode, "structured") {
					p.ObservedAt = ""
					p = Property{Target: "details", Type: TypeStructured, Subproperties: []Property{p}}
				}
				r := parityRule(tc.in, p)
				if err := Validate([]Rule{r}); err != nil {
					t.Fatalf("valid fixture: %v", err)
				}
				writes, err := NewEngine([]Rule{r}).Transform(tc.in)
				if err != nil || len(writes) != 1 {
					t.Fatalf("writes=%v err=%v", writes, err)
				}
				var entity map[string]any
				if err := json.Unmarshal([]byte(marshalWrite(t, writes[0])), &entity); err != nil {
					t.Fatal(err)
				}
				var got any
				if strings.HasPrefix(mode, "structured") {
					got = entity["details"].(map[string]any)["value"].(map[string]any)["result"]
				} else if attr, ok := entity["result"].(map[string]any); ok {
					got = attr["value"]
					if tc.typ == TypeRelationship {
						got = attr["object"]
					}
				}
				// DateTime has the profile's JSON-LD typed value in an attribute,
				// but Structured stores the converted scalar itself.
				if tc.typ == TypeDateTime && !strings.HasPrefix(mode, "structured") {
					if typed, ok := got.(map[string]any); ok {
						got = typed["@value"]
					}
				}
				actual, _ := json.Marshal(got)
				want, _ := json.Marshal(tc.want)
				if string(actual) != string(want) {
					t.Fatalf("got %s want %s", actual, want)
				}
			})
		}
	}
}

func TestExecutionParityStructuredTransformsAndRequired(t *testing.T) {
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"reading": "12"}}
	sp := Property{Target: "reading", Type: TypeNumber, Field: "reading", Transform: []Transform{{Op: OpMultiply, Value: new(float64(2))}}}
	r := parityRule(in, Property{Target: "details", Type: TypeStructured, Subproperties: []Property{sp}})
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	writes, err := NewEngine([]Rule{r}).Transform(in)
	if err != nil || len(writes) != 1 || !strings.Contains(marshalWrite(t, writes[0]), `"reading":24`) {
		t.Fatalf("expected converted transformed scalar: %v, %v", writes, err)
	}
	r.Entities[0].Properties[0].Subproperties[0].Required = true
	delete(in.Fields, "reading")
	writes, err = NewEngine([]Rule{r}).Transform(in)
	if err != nil || len(writes) != 0 {
		t.Fatalf("required subvalue must suppress entity: %v %v", writes, err)
	}
}

func TestExecutionParityValidationExpressionContext(t *testing.T) {
	in := Thing{ID: "object", Event: EventThingsV1Values}
	p := Property{Target: "details", Type: TypeStructured, Subproperties: []Property{{Target: "related", Type: TypeText, Value: "{{entities.unknown.id}}"}}}
	err := Validate([]Rule{parityRule(in, p)})
	for _, want := range []string{"parity-rule", "entity 0", "details.related", "{{entities.unknown.id}}"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("missing %q in %v", want, err)
		}
	}
}

func TestExecutionParityRejectsInactiveSelectorsAndDateMetadata(t *testing.T) {
	for _, p := range []Property{
		{Target: "reading", Type: TypeNumber, Const: 12, Env: "outside"},
		{Target: "reading", Type: TypeNumber, Derive: &Derive{Fn: FnSum, Of: []Source{{Const: 12, Object: "urn:example:object"}}}},
	} {
		if err := Validate([]Rule{parityRule(Measurement{}, p)}); err == nil {
			t.Fatalf("ignored configuration accepted: %+v", p)
		}
	}
}

func TestExecutionParityPrefixDoesNotSilentlyIgnoreTypes(t *testing.T) {
	p := Property{Target: "reading", Type: TypeNumber, Const: 12, Prefix: "urn:example:"}
	if err := Validate([]Rule{parityRule(Measurement{}, p)}); err == nil {
		t.Fatal("numeric constant silently ignores prefix")
	}
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"reading": []any{"ok", 12}}}
	p = Property{Target: "reading", Type: TypeTextArray, Field: "reading", Prefix: "prefix:"}
	r := parityRule(in, p)
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine([]Rule{r}).Transform(in); err == nil {
		t.Fatal("mixed dynamic list silently ignores prefix on numeric element")
	}
}

func TestExecutionParityGeoUsesTargetWithoutTime(t *testing.T) {
	in := Measurement{DeviceID: "sensor", HasLatLon: true, Lat: 62, Lon: 17}
	for _, source := range []Source{
		{LatLon: true},
		{Const: map[string]any{"latitude": 62, "longitude": 17}},
	} {
		r := parityRule(in, Property{Target: "position", Type: TypeGeo, Source: source})
		if err := Validate([]Rule{r}); err != nil {
			t.Fatal(err)
		}
		writes, err := NewEngine([]Rule{r}).Transform(in)
		if err != nil || len(writes) != 1 {
			t.Fatalf("%v %v", writes, err)
		}
		raw := marshalWrite(t, writes[0])
		if !strings.Contains(raw, `"position":{"type":"GeoProperty"`) || strings.Contains(raw, `"location"`) || strings.Contains(raw, `"observedAt"`) {
			t.Fatalf("unexpected Geo serialization: %s", raw)
		}
	}
}

func TestExecutionParityDynamicTypeErrors(t *testing.T) {
	for _, tc := range []struct {
		typ PropertyType
		bad any
	}{
		{TypeNumber, "not a number"}, {TypeText, map[string]any{"unexpected": true}},
		{TypeBoolean, "not a boolean"}, {TypeDateTime, "2026-02-30T12:00:00Z"},
		{TypeGeo, map[string]any{"type": "unsupported"}}, {TypeRelationship, []any{"urn:example:ok", true}},
		{TypeNumberArray, []any{1, "NaN"}}, {TypeTextArray, []any{"ok", map[string]any{}}},
		{TypeLanguageMap, map[string]any{"en": 12}},
	} {
		for _, structured := range []bool{false, true} {
			t.Run(string(tc.typ)+"/structured="+strconv.FormatBool(structured), func(t *testing.T) {
				in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"reading": tc.bad}}
				p := Property{Target: "reading", Type: tc.typ, Field: "reading"}
				if structured {
					p = Property{Target: "details", Type: TypeStructured, Subproperties: []Property{p}}
				}
				r := parityRule(in, p)
				if err := Validate([]Rule{r}); err != nil {
					t.Fatalf("dynamic rule must be valid: %v", err)
				}
				_, err := NewEngine([]Rule{r}).Transform(in)
				if err == nil || !strings.Contains(err.Error(), "parity-rule") || !strings.Contains(err.Error(), "reading") {
					t.Fatalf("expected contextual type error: %v", err)
				}
			})
		}
	}
}

func TestExecutionParityStructuredLanguageMap(t *testing.T) {
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"labels": map[string]any{"en": "Hello"}}}
	r := parityRule(in, Property{Target: "details", Type: TypeStructured, Subproperties: []Property{{Target: "labels", Type: TypeLanguageMap, Field: "labels"}}})
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	writes, err := NewEngine([]Rule{r}).Transform(in)
	if err != nil || len(writes) != 1 || !strings.Contains(marshalWrite(t, writes[0]), `"labels":{"en":"Hello"}`) {
		t.Fatalf("%v %v", writes, err)
	}
}

func TestExecutionParityRejectsIgnoredOptions(t *testing.T) {
	in := Thing{ID: "object", Event: EventThingsV1Values}
	for _, option := range []string{"unit", "observedAt", "observedBy"} {
		t.Run(option, func(t *testing.T) {
			sp := Property{Target: "reading", Type: TypeNumber, Field: "reading"}
			switch option {
			case "unit":
				sp.Unit = "CEL"
			case "observedAt":
				sp.ObservedAt = ObservedAtMessage
			case "observedBy":
				sp.ObservedBy = "urn:example:sensor"
			}
			r := parityRule(in, Property{Target: "details", Type: TypeStructured, Subproperties: []Property{sp}})
			if err := Validate([]Rule{r}); err == nil || !strings.Contains(err.Error(), "subproperty 0 (reading)") {
				t.Fatalf("expected contextual rejection, got %v", err)
			}
		})
	}
}

func TestExecutionParityErrorContext(t *testing.T) {
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"reading": "bad"}}
	p := Property{Target: "details", Type: TypeStructured, Subproperties: []Property{{Target: "reading", Type: TypeNumber, Value: "{{reading}}"}}}
	r := parityRule(in, p)
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	_, err := NewEngine([]Rule{r}).Transform(in)
	for _, want := range []string{"parity-rule", "entity 0", "details", "reading", "{{reading}}"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("missing %q in %v", want, err)
		}
	}
}
