package transform

import "testing"

func expressionRule() Rule {
	return Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values}, Entities: []Entity{{
		ID: "urn:example:{{id}}", Type: "Neutral",
		Properties: []Property{{Target: "label", Type: TypeText, Source: Source{Value: "{{name}}"}}},
	}}}
}

func TestMalformedExpressionsRejected(t *testing.T) {
	base := expressionRule()
	if err := Validate([]Rule{base}); err != nil {
		t.Fatal(err)
	}
	for _, expr := range []string{"{{id", "id}}", "{{}}", "{{safeURI()}}", "{{firstPresent()}}", "{{firstPresent(name,)}}", `{{field:values."unclosed.value}}`, "{{field:values..value}}", "{{field:values.x[-1]}}", "{{field:values.x[0]junk}}"} {
		t.Run(expr, func(t *testing.T) {
			rule := expressionRule()
			rule.Entities[0].Properties[0].Value = expr
			if err := Validate([]Rule{rule}); err == nil {
				t.Fatal("malformed expression accepted")
			}
		})
	}
}

func TestNestedFunctionsAndQuotedIndexedPaths(t *testing.T) {
	rule := expressionRule()
	rule.Entities[0].Properties[0].Value = `{{safeURI(firstPresent(field:values."a,b"[0][1], firstPresent(alternativeName,name)))}}`
	if err := Validate([]Rule{rule}); err != nil {
		t.Fatal(err)
	}
	in := Thing{Name: "fallback", Fields: map[string]any{"values": map[string]any{"a,b": []any{[]any{"zero", "A B/å"}}}}}
	got, err := renderTemplate(rule.Entities[0].Properties[0].Value, in, nil)
	if err != nil || got != "A%20B%2F%C3%A5" {
		t.Fatalf("nested expression: %q %v", got, err)
	}
	for _, path := range []string{`values."a,b"[0][1]`, `values."a,b"[0][0]`} {
		if _, ok := in.ResolvePath(path); !ok {
			t.Fatalf("accepted path not resolved: %s", path)
		}
	}
}

func TestQuotedPathEscapesAndDelimiters(t *testing.T) {
	in := Thing{Fields: map[string]any{"a}}b": map[string]any{"quote\"key": []any{"value"}}}}
	template := `{{field:"a}}b"."quote\"key"[0]}}`
	if err := validateTemplate(template); err != nil {
		t.Fatal(err)
	}
	got, err := renderTemplate(template, in, nil)
	if err != nil || got != "value" {
		t.Fatalf("quoted delimiters: %q %v", got, err)
	}
}

func TestDynamicTemplateFieldsAreValidatedAtRuntime(t *testing.T) {
	for _, variable := range []string{"deviceID", "sensorType", "object", "env"} {
		rule := expressionRule()
		rule.Entities[0].Properties[0].Value = "{{firstPresent(" + variable + ",name)}}"
		if err := Validate([]Rule{rule}); err != nil {
			t.Fatalf("dynamic thing field %s: %v", variable, err)
		}
		if got, err := renderTemplate(rule.Entities[0].Properties[0].Value, Thing{Name: "fallback"}, nil); err != nil || got != "fallback" {
			t.Fatalf("missing field did not use explicit fallback: %q %v", got, err)
		}
	}
	for _, variable := range []string{"id", "name", "nameOrID", "uuid", "type", "subType", "alternativeName"} {
		rule := validRule()
		rule.Entities[0].ID = "urn:example:{{safeURI(" + variable + ")}}"
		if err := Validate([]Rule{rule}); err != nil {
			t.Fatalf("dynamic measurement field %s: %v", variable, err)
		}
	}
	for _, name := range []string{"safeURI", "firstPresent"} {
		fn := templateFunctions[name]
		if fn.result != TypeText || fn.minArgs != 1 {
			t.Fatalf("undocumented function contract: %s %+v", name, fn)
		}
	}
}

func TestEntityIDsRequireDeclarationOrder(t *testing.T) {
	rule := expressionRule()
	first := rule.Entities[0]
	first.Name = "first"
	second := rule.Entities[0]
	second.Name = "second"
	second.ID = "urn:example:second"
	rule.Entities = []Entity{first, second}
	if err := Validate([]Rule{rule}); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	rule.Entities[0].ID = "{{entities.second.id}}"
	if err := Validate([]Rule{rule}); err == nil {
		t.Fatal("forward ID reference accepted for non-create entity")
	}
	rule.Entities[0].ID = "{{entities.first.id}}"
	if err := Validate([]Rule{rule}); err == nil {
		t.Fatal("self-referential ID accepted")
	}
	rule.Entities[0].ID = "urn:example:first"
	rule.Entities[1].Properties[0].Value = "{{entities.first.id}}"
	if err := Validate([]Rule{rule}); err != nil {
		t.Fatalf("valid backward reference: %v", err)
	}
	if _, err := NewEngine([]Rule{rule}).Transform(Thing{Event: EventThingsV1Values, Name: "value"}); err != nil {
		t.Fatalf("accepted reference failed at runtime: %v", err)
	}
}

func TestReferencesValidatedAtEveryExpressionSite(t *testing.T) {
	sites := map[string]func(*Rule, string){
		"type": func(r *Rule, expr string) { r.Entities[0].Type = expr },
		"observedBy": func(r *Rule, expr string) {
			r.Entities[0].Properties = []Property{{Target: "reading", Type: TypeNumber, Source: Source{Field: "reading"}, ObservedBy: expr}}
		},
		"subproperty": func(r *Rule, expr string) {
			r.Entities[0].Properties = []Property{{Target: "structured", Type: TypeStructured, Subproperties: []Property{{Target: "label", Type: TypeText, Source: Source{Value: expr}}}}}
		},
	}
	for site, set := range sites {
		t.Run(site, func(t *testing.T) {
			rule := expressionRule()
			set(&rule, "urn:example:neutral")
			if err := Validate([]Rule{rule}); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			set(&rule, "{{ firstPresent(entities.unknown.id, name) }}")
			if err := Validate([]Rule{rule}); err == nil {
				t.Fatal("unknown nested reference accepted")
			}
		})
	}
}

func TestObservedByCanUseRenderedEntityID(t *testing.T) {
	rule := expressionRule()
	rule.Entities[0].Name = "self"
	rule.Entities[0].Properties = []Property{{Target: "reading", Type: TypeNumber, Source: Source{Field: "reading"}, ObservedBy: "{{ entities.self.id }}"}}
	if err := Validate([]Rule{rule}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine([]Rule{rule}).Transform(Thing{ID: "neutral", Event: EventThingsV1Values, Fields: map[string]any{"reading": 4.0}}); err != nil {
		t.Fatalf("accepted observedBy reference failed: %v", err)
	}
	other := rule.Entities[0]
	other.Name = "other"
	other.ID = "{{entities.self.id}}"
	rule.Entities = append(rule.Entities, other)
	rule.Entities[0].ID = "{{ firstPresent(entities.other.id,id) }}"
	if err := Validate([]Rule{rule}); err == nil {
		t.Fatal("cyclic nested entity IDs accepted")
	}
}

func TestUnsupportedPropertyCombinationsRejected(t *testing.T) {
	cases := []Property{
		{Target: "position", Type: TypeText, Source: Source{LatLon: true}},
		{Target: "position", Type: TypeGeo, Source: Source{LatLon: true, Prefix: "ignored"}},
		{Target: "labels", Type: TypeLanguageMap, Source: Source{Const: map[string]any{"en": "text"}}, ObservedAt: ObservedAtMessage},
		{Target: "labels", Type: TypeLanguageMap, Source: Source{Const: map[string]any{"en": "text"}, Prefix: "ignored"}},
		{Target: "structure", Type: TypeStructured, Source: Source{Const: "ignored"}, Subproperties: []Property{{Target: "label", Type: TypeText, Source: Source{Const: "text"}}}},
	}
	for _, property := range cases {
		rule := expressionRule()
		rule.Entities[0].Properties[0] = property
		if err := Validate([]Rule{rule}); err == nil {
			t.Fatalf("ignored combination accepted: %+v", property)
		}
	}
}

func TestAttributeConflictsRejected(t *testing.T) {
	for _, target := range []string{"id", "type", "@context"} {
		rule := expressionRule()
		rule.Entities[0].Properties[0].Target = target
		if err := Validate([]Rule{rule}); err == nil {
			t.Fatalf("reserved target %s accepted", target)
		}
	}
	rule := expressionRule()
	rule.Entities[0].Properties = append(rule.Entities[0].Properties, Property{Target: "label", Type: TypeNumber, Source: Source{Const: 2.0}})
	if err := Validate([]Rule{rule}); err == nil {
		t.Fatal("duplicate/conflicting attribute accepted")
	}
	rule = expressionRule()
	rule.Entities[0].Properties = nil
	rule.Entities[0].RemoveAttributes = []string{"label", "label"}
	if err := Validate([]Rule{rule}); err == nil {
		t.Fatal("duplicate attribute removal accepted")
	}
	rule.Entities[0].RemoveAttributes = []string{"id"}
	if err := Validate([]Rule{rule}); err == nil {
		t.Fatal("structural ID removal accepted")
	}
}
