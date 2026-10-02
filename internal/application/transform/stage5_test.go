package transform

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func stage5Rule(in Input, properties ...Property) Rule {
	r := Rule{ID: "stage5", Match: Match{Kind: in.Kind()}, Entities: []Entity{{ID: "urn:example:{{id}}", Type: "Generic", Properties: properties}}}
	if in.Kind() == KindThing {
		r.Match.Event = in.MatchInfo().Event
		r.Match.Type = in.MatchInfo().Type
		r.Match.Lifecycle = in.MatchInfo().Lifecycle
	}
	return r
}

func TestStage5RankingDiagnosticsAndRelationTarget(t *testing.T) {
	in := Thing{ID: "object", Tenant: "tenant-a", Type: "source", Event: EventThingsV1Relations, Relation: "partOf", Fields: map[string]any{"relation": map[string]any{"to": map[string]any{"type": "target", "typeName": "different-shortcut"}}}}
	base := stage5Rule(in, Property{Target: "label", Type: TypeText, Const: "generic"})
	base.ID = "generic"
	specific := stage5Rule(in, Property{Target: "label", Type: TypeText, Const: "specific"})
	specific.ID, specific.Match.ToType, specific.Match.Tenant = "specific", "target", "tenant-a"
	foreign := base
	foreign.ID, foreign.Priority, foreign.Match.Tenant = "foreign", 100, "tenant-b"
	duplicate := specific
	duplicate.ID = "later"
	rules := []Rule{base, specific, foreign, duplicate}
	if err := Validate(rules); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(rules)
	report := engine.ExplainMatch(in)
	if len(report) != 3 || !report[1].Winner || report[2].Reason != "later snapshot order" {
		t.Fatalf("ranking diagnostics wrong: %+v", report)
	}
	writes, err := engine.Transform(in)
	if err != nil || len(writes) != 1 || writes[0].RuleID != "specific" {
		t.Fatalf("winner: %v %v", writes, err)
	}
	if len(engine.PotentialOverlaps()) != 4 {
		t.Fatalf("unexpected overlaps: %+v", engine.PotentialOverlaps())
	}
	in.Fields["relation"].(map[string]any)["to"].(map[string]any)["type"] = "other-target"
	writes, err = NewEngine([]Rule{specific}).Transform(in)
	if err != ErrNoMatchingRule || len(writes) != 0 {
		t.Fatalf("target type crossed: %v %v", writes, err)
	}
	for _, priority := range []int{-1, 1} {
		specific.Priority = priority
		in.Fields["relation"].(map[string]any)["to"].(map[string]any)["type"] = "target"
		writes, err = NewEngine([]Rule{base, specific}).Transform(in)
		want := "specific"
		if priority < 0 {
			want = "generic"
		}
		if err != nil || writes[0].RuleID != want {
			t.Fatalf("signed priority %d: %v %v", priority, writes, err)
		}
	}
}

func TestStage5TimesAcrossPropertyTypesAndMissingTime(t *testing.T) {
	observed := "2026-10-01T12:00:00.123456789Z"
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"event": map[string]any{"publishedAt": observed}, "sources": map[string]any{}}}
	sources := in.Fields["sources"].(map[string]any)
	for _, tc := range []struct {
		typ   PropertyType
		value any
	}{
		{TypeNumber, 0.0}, {TypeBoolean, false}, {TypeText, "text"}, {TypeDateTime, observed},
		{TypeGeo, map[string]any{"type": "Point", "coordinates": []any{17.0, 62.0}}},
		{TypeRelationship, "urn:example:related"}, {TypeTextArray, []any{"text"}}, {TypeNumberArray, []any{0.0, 1.0}},
		{TypeLanguageMap, map[string]any{"en": "text"}},
	} {
		t.Run(string(tc.typ), func(t *testing.T) {
			sources["reading"] = map[string]any{"value": tc.value, "observedAt": observed}
			p := Property{Target: "reading", Type: tc.typ, Field: "sources.reading.value"}
			for _, mode := range []ObservedAt{"", ObservedAtRecord, ObservedAtMessage, ObservedAt("field:event.publishedAt")} {
				p.ObservedAt = mode
				output := stage5Output(t, in, stage5Rule(in, p))
				if output["reading"].(map[string]any)["observedAt"] != observed {
					t.Fatalf("%s time=%v", mode, output)
				}
			}
			p.ObservedAt = ObservedAtNone
			if _, exists := stage5Output(t, in, stage5Rule(in, p))["reading"].(map[string]any)["observedAt"]; exists {
				t.Fatal("none wrote time")
			}
		})
	}
	sources["reading"] = map[string]any{"value": 1.0, "observedAt": observed}
	out := stage5Output(t, in, stage5Rule(in, Property{Target: "details", Type: TypeStructured, Subproperties: []Property{{Target: "reading", Type: TypeNumber, Field: "sources.reading.value"}}}))
	if out["details"].(map[string]any)["observedAt"] != observed {
		t.Fatalf("Structured time: %v", out)
	}
	sources["reading"] = map[string]any{"value": 1.0}
	in.ObservedAt = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	delete(in.Fields, "event")
	out = stage5Output(t, in, stage5Rule(in, Property{Target: "reading", Type: TypeNumber, Field: "sources.reading.value"}))
	if _, exists := out["reading"].(map[string]any)["observedAt"]; exists {
		t.Fatal("missing value time was replaced by another observation")
	}
}

func TestStage5TemplateAndAggregateProvenance(t *testing.T) {
	first := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	last := first.Add(time.Minute)
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"values": map[string]any{
		"first": map[string]any{"value": 0.0, "observedAt": sourceTime(first), "unit": "Cel"},
		"last":  map[string]any{"value": 2.0, "observedAt": sourceTime(last), "unit": "CEL"},
	}}}
	for _, fn := range []string{FnSum, FnMin, FnMax, FnFirstPresent, FnCoalesce} {
		p := Property{Target: "reading", Type: TypeNumber, Derive: &Derive{Fn: fn, Of: []Source{{Field: "missing"}, {Field: "values.first.value"}, {Field: "values.last.value"}}}}
		want := last
		if fn == FnFirstPresent || fn == FnCoalesce {
			want = first
		}
		output := stage5Output(t, in, stage5Rule(in, p))
		if output["reading"].(map[string]any)["observedAt"] != sourceTime(want) {
			t.Fatalf("%s lost provenance: %v", fn, output)
		}
	}
	for _, template := range []string{"{{values.first.value}}", "{{firstPresent(missing,values.first.value)}}"} {
		p := Property{Target: "reading", Type: TypeNumber, Value: template, Unit: "KEL"}
		r := stage5Rule(in, p)
		if err := Validate([]Rule{r}); err != nil {
			t.Fatal(err)
		}
		if _, err := NewEngine([]Rule{r}).Transform(in); err == nil {
			t.Fatal("template stripped source unit")
		}
		p.Unit = "CEL"
		out := stage5Output(t, in, stage5Rule(in, p))
		if out["reading"].(map[string]any)["observedAt"] != sourceTime(first) {
			t.Fatalf("template lost selected time: %v", out)
		}
	}
}

func TestStage5ExplicitConversionsAreValidatedAndImmutable(t *testing.T) {
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"source": map[string]any{"value": []float64{0, 12}, "unit": "Cel"}}}
	p := Property{Target: "readings", Type: TypeNumberArray, Field: "source.value", Convert: &Conversion{From: "Cel", To: "KEL", Offset: 273.15}}
	r := stage5Rule(in, p)
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine([]Rule{r})
	for range 2 {
		writes, err := engine.Transform(in)
		if err != nil || len(writes) != 1 {
			t.Fatalf("%v %v", writes, err)
		}
		var output map[string]any
		if err := json.Unmarshal([]byte(marshalWrite(t, writes[0])), &output); err != nil {
			t.Fatal(err)
		}
		entry := output["readings"].(map[string]any)
		if entry["unitCode"] != "KEL" || !reflect.DeepEqual(entry["value"], []any{273.15, 285.15}) {
			t.Fatalf("conversion: %v", entry)
		}
	}
	if !reflect.DeepEqual(in.Fields["source"].(map[string]any)["value"], []float64{0, 12}) {
		t.Fatal("conversion mutated source")
	}
	for _, conversion := range []*Conversion{
		{To: "KEL"}, {From: "Cel", To: "KEL"}, {From: "Cel", To: "KEL", Factor: new(math.Inf(1))}, {From: "Cel", To: "KEL", Offset: math.NaN()},
	} {
		r.Entities[0].Properties[0].Convert = conversion
		if err := Validate([]Rule{r}); err == nil {
			t.Fatalf("invalid conversion accepted: %+v", conversion)
		}
	}
	r.Entities[0].Properties[0].Convert = &Conversion{From: "m", To: "MMT", Factor: new(1000.0)}
	if _, err := NewEngine([]Rule{r}).Transform(in); err == nil {
		t.Fatal("mismatched source unit accepted")
	}
}

func stage5Output(t *testing.T, in Input, rule Rule) map[string]any {
	t.Helper()
	if err := Validate([]Rule{rule}); err != nil {
		t.Fatalf("valid regression rule: %v", err)
	}
	writes, err := NewEngine([]Rule{rule}).Transform(in)
	if err != nil || len(writes) != 1 {
		t.Fatalf("writes=%v err=%v", writes, err)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(marshalWrite(t, writes[0])), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestStage5PriorityAndEnvironment(t *testing.T) {
	in := Measurement{DeviceID: "sensor", Env: "outside", Port: "0"}
	generic := stage5Rule(in, Property{Target: "chosen", Type: TypeText, Const: "priority"})
	raw, _ := json.Marshal(generic)
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	fields["priority"] = 10
	raw, _ = json.Marshal(fields)
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	specific := stage5Rule(in, Property{Target: "chosen", Type: TypeText, Const: "specific"})
	specific.Match.Port = "0"
	if err := Validate([]Rule{generic, specific}); err != nil {
		t.Fatal(err)
	}
	writes, err := NewEngine([]Rule{generic, specific}).Transform(in)
	if err != nil || len(writes) != 1 || !strings.Contains(marshalWrite(t, writes[0]), `"value":"priority"`) {
		t.Fatalf("priority lost to specificity: %v %v", writes, err)
	}
	generic = stage5Rule(in, Property{Target: "chosen", Type: TypeText, Const: "generic"})
	specific.Match.Port, specific.Match.Env = "", "outside"
	writes, err = NewEngine([]Rule{generic, specific}).Transform(in)
	if err != nil || !strings.Contains(marshalWrite(t, writes[0]), `"value":"specific"`) {
		t.Fatalf("env specificity ignored: %v %v", writes, err)
	}
}

func TestStage5RejectsUnavailableMatchFields(t *testing.T) {
	in := Thing{ID: "object", Event: EventThingsV1Values}
	r := stage5Rule(in, Property{Target: "reading", Type: TypeNumber, Const: 1})
	r.Match.Device = "sensor"
	if err := Validate([]Rule{r}); err == nil {
		t.Fatal("things cannot supply match.device")
	}
	r.Match.Device, r.Match.Event = "", EventThingsV1Lifecycle
	if err := Validate([]Rule{r}); err == nil {
		t.Fatal("lifecycle requires explicit kind")
	}
}

func TestStage5EmptySourcesAndMissingTemplates(t *testing.T) {
	for _, value := range []any{nil, "", []any{}, map[string]any{}} {
		in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"reading": value}}
		for _, required := range []bool{false, true} {
			r := stage5Rule(in, Property{Target: "reading", Type: TypeNumber, Field: "reading", Required: required})
			if err := Validate([]Rule{r}); err != nil {
				t.Fatal(err)
			}
			writes, err := NewEngine([]Rule{r}).Transform(in)
			if err != nil || len(writes) != 0 {
				t.Errorf("empty %T required=%t: %v %v", value, required, writes, err)
			}
		}
	}
	in := Thing{ID: "object", Event: EventThingsV1Values}
	r := stage5Rule(in, Property{Target: "reading", Type: TypeText, Value: "prefix:{{missing}}"})
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	if writes, err := NewEngine([]Rule{r}).Transform(in); err != nil || len(writes) != 0 {
		t.Errorf("missing template must skip: %v %v", writes, err)
	}
}

func TestStage5AggregatesMissingAndTypeErrors(t *testing.T) {
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"good": 1.0, "bad": map[string]any{"not": "scalar"}}}
	for _, fn := range KnownDeriveFns() {
		r := stage5Rule(in, Property{Target: "reading", Type: TypeNumber, Derive: &Derive{Fn: fn, Of: []Source{{Field: "missing"}}}})
		if err := Validate([]Rule{r}); err != nil {
			t.Fatal(err)
		}
		if writes, err := NewEngine([]Rule{r}).Transform(in); err != nil || len(writes) != 0 {
			t.Errorf("%s must not fabricate empty result: %v %v", fn, writes, err)
		}
		if fn == FnFirstPresent || fn == FnCoalesce {
			continue
		}
		r.Entities[0].Properties[0].Derive.Of = []Source{{Field: "good"}, {Field: "bad"}}
		if _, err := NewEngine([]Rule{r}).Transform(in); err == nil {
			t.Errorf("%s ignored bad dynamic input", fn)
		}
	}
}

func TestStage5OwnObservationTimeAndConstants(t *testing.T) {
	observed := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	published := observed.Add(time.Hour)
	in := Thing{ID: "object", Event: EventThingsV1Values, ObservedAt: observed.Add(time.Minute), Fields: map[string]any{
		"event":  map[string]any{"publishedAt": published.Format(time.RFC3339Nano)},
		"values": map[string]any{"reading": map[string]any{"value": 12.0, "observedAt": observed.Format(time.RFC3339Nano), "unit": "Cel"}},
	}}
	out := stage5Output(t, in, stage5Rule(in,
		Property{Target: "reading", Type: TypeNumber, Field: "values.reading.value"},
		Property{Target: "constant", Type: TypeNumber, Const: 0},
		Property{Target: "published", Type: TypeDateTime, Value: "{{event.publishedAt}}"},
	))
	if got := out["reading"].(map[string]any)["observedAt"]; got != observed.Format(time.RFC3339Nano) {
		t.Errorf("own time=%v", got)
	}
	if _, set := out["constant"].(map[string]any)["observedAt"]; set {
		t.Error("constant acquired measurement time")
	}
	if !in.MessageTime().Equal(published) {
		t.Errorf("published time aliased to observation: %v", in.MessageTime())
	}
	m := Measurement{DeviceID: "sensor", Timestamp: published, HeaderTime: observed.Add(-time.Minute), HasHeaderTime: true, Records: []Record{{Name: "5700", Value: new(12.0), Time: observed, HasTime: true}}}
	out = stage5Output(t, m, stage5Rule(m, Property{Target: "reading", Type: TypeNumber, Resource: "5700"}))
	if got := out["reading"].(map[string]any)["observedAt"]; got != observed.Format(time.RFC3339Nano) {
		t.Errorf("measurement time=%v", got)
	}
}

func TestStage5RejectsUnitRelabelWithoutConversion(t *testing.T) {
	in := Measurement{DeviceID: "sensor", Records: []Record{{Name: "5700", Value: new(12.0), Unit: "Cel"}}}
	r := stage5Rule(in, Property{Target: "reading", Type: TypeNumber, Resource: "5700", Unit: "KEL"})
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine([]Rule{r}).Transform(in); err == nil {
		t.Fatal("source Cel relabeled KEL without conversion")
	}
}

func TestStage5HeaderFallbackAndInvalidFieldTime(t *testing.T) {
	header := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	m := Measurement{DeviceID: "sensor", HeaderTime: header, HasHeaderTime: true, Records: []Record{{Name: "5700", Value: new(0.0)}}}
	for _, p := range []Property{
		{Target: "reading", Type: TypeNumber, Resource: "5700"},
		{Target: "reading", Type: TypeNumber, Value: "{{records[0].value}}"},
		{Target: "reading", Type: TypeNumber, Value: "{{5700}}"},
	} {
		out := stage5Output(t, m, stage5Rule(m, p))
		if out["reading"].(map[string]any)["observedAt"] != sourceTime(header) {
			t.Fatalf("missing header fallback: %+v", out)
		}
	}
	m.HeaderTime = time.Time{}
	out := stage5Output(t, m, stage5Rule(m, Property{Target: "reading", Type: TypeNumber, Resource: "5700"}))
	if _, exists := out["reading"].(map[string]any)["observedAt"]; exists {
		t.Fatal("zero time fabricated")
	}
	in := Thing{ID: "object", Event: EventThingsV1Values, Fields: map[string]any{"badTime": "not-a-date"}}
	r := stage5Rule(in, Property{Target: "reading", Type: TypeNumber, Const: 1, ObservedAt: ObservedAt("field:badTime")})
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine([]Rule{r}).Transform(in); err == nil {
		t.Fatal("invalid field time treated as missing")
	}
}
