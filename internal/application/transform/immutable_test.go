package transform

import "testing"

func TestEngineOwnsImmutableConfiguration(t *testing.T) {
	factor := 2.0
	constant := map[string]any{"en": "original"}
	rules := []Rule{{
		ID: "rule-1", Revision: 7, SnapshotVersion: 11,
		Match: Match{Kind: KindThing, Event: EventThingsV1Values},
		Entities: []Entity{{ID: "urn:example:source", Type: "Neutral", Properties: []Property{
			{Target: "reading", Type: TypeNumber, Source: Source{Field: "reading"}, Transform: []Transform{{Op: OpMultiply, Value: &factor}}},
			{Target: "label", Type: TypeLanguageMap, Source: Source{Const: constant}},
		}}},
	}}
	if err := Validate(rules); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(rules)
	// Ändra alla referensbärande nivåer efter att motorn har skapats.
	factor = 100
	constant["en"] = "changed"
	rules[0].Entities[0].Properties[0].Target = "changed"
	rules[0].Entities[0].ID = "urn:example:changed"
	rules[0].ID = "changed"
	writes, err := engine.Transform(Thing{Event: EventThingsV1Values, Fields: map[string]any{"reading": 3.0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 || writes[0].EntityID != "urn:example:source" || writes[0].RuleID != "rule-1" || writes[0].RuleRevision != 7 || writes[0].SnapshotVersion != 11 {
		t.Fatalf("caller mutation changed engine: %+v", writes)
	}
	property := engine.rules[0].Entities[0].Properties[0]
	if property.Target != "reading" || *property.Transform[0].Value != 2 || engine.rules[0].Entities[0].Properties[1].Const.(map[string]any)["en"] != "original" {
		t.Fatal("nested caller data is still shared")
	}
}
