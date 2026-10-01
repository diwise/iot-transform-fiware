package transform

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
)

func TestStaticRuleValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Entity)
		want   string
	}{
		{"id", func(e *Entity) { e.ID = "relative-id" }, "entity.id"},
		{"id whitespace", func(e *Entity) { e.ID = "urn:example:bad id" }, "entity.id"},
		{"type", func(e *Entity) { e.Type = "bad type" }, "entity.type"},
		{"context", func(e *Entity) { e.Context = "relative-context" }, "entity.context"},
		{"divide zero", func(e *Entity) { e.Properties[0].Transform = []Transform{{Op: OpDivide, Value: ptr(0)}} }, "divide by zero"},
		{"infinite argument", func(e *Entity) { e.Properties[0].Transform = []Transform{{Op: OpAdd, Value: ptr(math.Inf(1))}} }, "finite"},
		{"nan argument", func(e *Entity) { e.Properties[0].Transform = []Transform{{Op: OpMultiply, Value: ptr(math.NaN())}} }, "finite"},
		{"number", func(e *Entity) { e.Properties[0].Source = Source{Const: "no-number"} }, "expected number"},
		{"nonfinite location", func(e *Entity) {
			e.Properties[0] = Property{Target: "position", Type: TypeGeo, Source: Source{Const: map[string]any{"latitude": math.Inf(1), "longitude": 12}}}
		}, "expected a location"},
		{"nonfinite geojson", func(e *Entity) {
			e.Properties[0] = Property{Target: "position", Type: TypeGeo, Source: Source{Const: map[string]any{"type": "Point", "coordinates": []any{12, math.NaN()}}}}
		}, "expected a location"},
		{"number overflow", func(e *Entity) {
			e.Properties[0].Source = Source{Const: math.MaxFloat64}
			e.Properties[0].Transform = []Transform{{Op: OpMultiply, Value: ptr(2)}}
		}, "finite"},
		{"date", func(e *Entity) {
			e.Properties[0] = Property{Target: "sampled", Type: TypeDateTime, Source: Source{Const: "2026-02-30T12:00:00Z"}}
		}, "date time"},
		{"literal date", func(e *Entity) {
			e.Properties[0] = Property{Target: "sampled", Type: TypeDateTime, Source: Source{Value: "yesterday"}}
		}, "date time"},
		{"relationship", func(e *Entity) {
			e.Properties[0] = Property{Target: "related", Type: TypeRelationship, Source: Source{Const: []any{"urn:example:ok", true}}}
		}, "relationship"},
		{"observedBy", func(e *Entity) { e.Properties[0].ObservedBy = "not-a-uri" }, "observedBy"},
		{"number list", func(e *Entity) {
			e.Properties[0] = Property{Target: "samples", Type: TypeNumberArray, Source: Source{Const: []any{1, "NaN"}}}
		}, "finite"},
		{"language map", func(e *Entity) {
			e.Properties[0] = Property{Target: "label", Type: TypeLanguageMap, Source: Source{Const: map[string]any{"en": 42}}}
		}, "language map"},
		{"transform input", func(e *Entity) {
			e.Properties[0] = Property{Target: "state", Type: TypeText, Source: Source{Const: 42}, Transform: []Transform{{Op: OpBoolToOnOff}}}
		}, "expects a boolean"},
		{"derive input", func(e *Entity) {
			e.Properties[0] = Property{Target: "total", Type: TypeNumber, Derive: &Derive{Fn: FnSum, Of: []Source{{Const: "bad"}, {Resource: "5700"}}}}
		}, "derive.of 0"},
		{"derive result", func(e *Entity) {
			e.Properties[0] = Property{Target: "total", Type: TypeNumber, Derive: &Derive{Fn: FnSum, Of: []Source{{Const: math.MaxFloat64}, {Const: math.MaxFloat64}}}}
		}, "finite"},
		{"subproperty", func(e *Entity) {
			e.Properties = []Property{{Target: "details", Type: TypeStructured, Subproperties: []Property{{Target: "sampled", Type: TypeDateTime, Source: Source{Const: "bad"}}}}}
		}, "subproperty 0 (sampled)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRule()
			r.ID = "static-test"
			if err := Validate([]Rule{r}); err != nil {
				t.Fatalf("regression fixture must be valid: %v", err)
			}
			tt.change(&r.Entities[0])
			err := Validate([]Rule{r})
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), "static-test") {
				t.Fatalf("expected contextual error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func TestStaticValidationAcceptsGenericMappings(t *testing.T) {
	r := validRule()
	r.Entities[0].Context = "https://example.org/context.jsonld"
	r.Entities[0].Type = "https://example.org/ArbitraryModel"
	r.Entities[0].Properties = []Property{
		{Target: "zero", Type: TypeNumber, Source: Source{Const: 0}},
		{Target: "false", Type: TypeBoolean, Source: Source{Const: false}},
		{Target: "date", Type: TypeDateTime, Source: Source{Value: "2026-10-01T12:00:00.123456+02:00"}},
		{Target: "relation", Type: TypeRelationship, Array: true, Source: Source{Const: "source", Prefix: "urn:example:"}},
		{Target: "dynamic", Type: TypeDateTime, Source: Source{Value: "{{timestamp}}"}},
		{Target: "encoded", Type: TypeRelationship, Source: Source{Const: "a b", Prefix: "urn:example:"}, Transform: []Transform{{Op: OpSafeURI}}},
	}
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	writes, err := NewEngine([]Rule{r}).Transform(Measurement{Timestamp: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)})
	if err != nil || len(writes) != 1 {
		t.Fatalf("expected one write, got %v, %v", writes, err)
	}
	e, err := entities.New(writes[0].EntityID, writes[0].TypeName, writes[0].Props...)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	if err := json.Unmarshal(b, &output); err != nil {
		t.Fatal(err)
	}
	if output["zero"].(map[string]any)["value"] != float64(0) || output["false"].(map[string]any)["value"] != false {
		t.Fatalf("lost zero or false: %s", b)
	}
	if output["encoded"].(map[string]any)["object"] != "urn:example:a%20b" {
		t.Fatalf("transform/prefix diverged from static validation: %s", b)
	}
}

func TestDynamicValuesUseSharedProtocolChecks(t *testing.T) {
	for _, tt := range []struct {
		typeName PropertyType
		raw      any
	}{
		{TypeDateTime, "not-a-date"},
		{TypeNumber, math.Inf(1)},
		{TypeNumberArray, []any{1, math.NaN()}},
	} {
		t.Run(string(tt.typeName), func(t *testing.T) {
			r := validRule()
			r.Match = Match{Kind: KindThing, Event: EventThingsV1Values}
			r.Entities[0].Properties = []Property{{Target: "sample", Type: tt.typeName, Source: Source{Field: "sample"}}}
			if err := Validate([]Rule{r}); err != nil {
				t.Fatal(err)
			}
			_, err := NewEngine([]Rule{r}).Transform(Thing{Event: EventThingsV1Values, Fields: map[string]any{"sample": tt.raw}})
			if err == nil {
				t.Fatal("dynamic invalid value must fail execution")
			}
		})
	}
}
