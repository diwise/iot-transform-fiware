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

func TestTemplateVariablesAreSourceSpecific(t *testing.T) {
	for _, variable := range []string{"deviceID", "sensorType", "object", "env"} {
		rule := expressionRule()
		rule.Entities[0].Properties[0].Value = "{{firstPresent(" + variable + ",name)}}"
		if err := Validate([]Rule{rule}); err == nil {
			t.Fatalf("thing accepted measurement variable %s", variable)
		}
	}
	for _, variable := range []string{"id", "name", "nameOrID", "uuid", "type", "subType", "alternativeName"} {
		rule := validRule()
		rule.Entities[0].ID = "urn:example:{{safeURI(" + variable + ")}}"
		if err := Validate([]Rule{rule}); err == nil {
			t.Fatalf("measurement accepted thing variable %s", variable)
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
