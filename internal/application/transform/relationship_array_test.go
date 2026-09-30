package transform

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRelationshipArrayJSON(t *testing.T) {
	for _, test := range []struct {
		name   string
		raw    any
		array  bool
		prefix string
		want   any
	}{
		{"scalar", "urn:example:one", false, "", "urn:example:one"},
		{"wrapped scalar", "urn:example:one", true, "", []any{"urn:example:one"}},
		{"flat list", []any{"urn:example:one", "urn:example:two"}, true, "", []any{"urn:example:one", "urn:example:two"}},
		{"typed list prefix", []string{"one", "two"}, true, "urn:example:", []any{"urn:example:one", "urn:example:two"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rule := expressionRule()
			rule.Entities[0].Properties = []Property{{Target: "links", Type: TypeRelationship, Array: test.array, Source: Source{Const: test.raw, Prefix: test.prefix}, ObservedAt: ObservedAtMessage}}
			if err := Validate([]Rule{rule}); err != nil {
				t.Fatal(err)
			}
			writes, err := NewEngine([]Rule{rule}).Transform(Thing{ID: "neutral", Event: EventThingsV1Values})
			if err != nil {
				t.Fatal(err)
			}
			var entity map[string]any
			if err := json.Unmarshal([]byte(marshalWrite(t, writes[0])), &entity); err != nil {
				t.Fatal(err)
			}
			property := entity["links"].(map[string]any)
			if property["type"] != "Relationship" || !reflect.DeepEqual(property["object"], test.want) {
				t.Fatalf("wrong actual JSON: %+v", property)
			}
		})
	}
}

func TestRelationshipReferencesAreStrict(t *testing.T) {
	for _, raw := range []any{17.0, true, "relative", "urn:", "urn:example:has space", []any{"urn:example:valid", 17.0}, []any{[]any{"urn:example:nested"}}} {
		rule := expressionRule()
		rule.Entities[0].Properties = []Property{{Target: "links", Type: TypeRelationship, Array: true, Source: Source{Field: "references"}}}
		if err := Validate([]Rule{rule}); err != nil {
			t.Fatalf("valid regression rule: %v", err)
		}
		if _, err := NewEngine([]Rule{rule}).Transform(Thing{ID: "neutral", Event: EventThingsV1Values, Fields: map[string]any{"references": raw}}); err == nil {
			t.Fatalf("invalid relationship silently accepted: %#v", raw)
		}
	}
	rule := expressionRule()
	rule.Entities[0].Properties[0].Array = true
	if err := Validate([]Rule{rule}); err == nil {
		t.Fatal("array accepted on non-Relationship target")
	}
}

func TestRelationshipArrayIsNotIgnoredInStructuredSubvalues(t *testing.T) {
	rule := expressionRule()
	rule.Entities[0].Properties = []Property{{Target: "document", Type: TypeStructured, Subproperties: []Property{{Target: "reference", Type: TypeRelationship, Source: Source{Const: "urn:example:target"}}}}}
	if err := Validate([]Rule{rule}); err != nil {
		t.Fatalf("otherwise valid rule: %v", err)
	}
	rule.Entities[0].Properties[0].Subproperties[0].Array = true
	if err := Validate([]Rule{rule}); err == nil {
		t.Fatal("unsupported array modifier silently accepted for structured subvalue")
	}
}
