package transform

import (
	"strings"
	"testing"
)

func ruleSetFixture(match Match, typeName string) Rule {
	return Rule{Match: match, Entities: []Entity{{
		ID: "urn:example:{{id}}", Type: typeName,
		Properties: []Property{{Target: "sample", Type: TypeNumber, Const: 1}},
	}}}
}

func TestRuleSetMatchConflicts(t *testing.T) {
	m := func(tenant string) Match { return Match{Kind: KindMeasurement, Tenant: tenant} }
	thing := func(event, sourceType, lifecycle string, removed bool) Match {
		m := Match{Kind: KindThing, Event: event, Type: sourceType, Lifecycle: lifecycle, RelationRemoved: removed}
		if event == EventThingsV1Relations {
			m.Relation = "linkedTo"
		}
		return m
	}
	type conflictCase struct {
		name     string
		a, b     Match
		conflict bool
	}
	tests := []conflictCase{
		{"separate tenants", m("a"), m("b"), false},
		{"same tenant", m("a"), m("a"), true},
		{"global and tenant", m(""), m("a"), true},
		{"global rules", m(""), m(""), true},
		{"different kinds", m(""), thing(EventThingsV1Values, "", "", false), false},
		{"different events", thing(EventThingsV1Values, "Source", "", false), thing(EventThingsV1Lifecycle, "Source", "created", false), false},
		{"source types", thing(EventThingsV1Values, "First", "", false), thing(EventThingsV1Values, "Second", "", false), false},
		{"source wildcard", thing(EventThingsV1Values, "", "", false), thing(EventThingsV1Values, "Source", "", false), true},
		{"relation removal", thing(EventThingsV1Relations, "Source", "", false), thing(EventThingsV1Relations, "Source", "", true), false},
		{"lifecycle", thing(EventThingsV1Lifecycle, "Source", "created", false), thing(EventThingsV1Lifecycle, "Source", "deleted", false), false},
		{"updated lifecycle is exact", thing(EventThingsV1Lifecycle, "Source", "updated", false), thing(EventThingsV1Lifecycle, "Source", "created", false), false},
		{"different resources can coexist", Match{Kind: KindMeasurement, Resource: "5700"}, Match{Kind: KindMeasurement, Resource: "5701"}, true},
		{"sensor case folding", Match{Kind: KindMeasurement, SensorType: "Sensor"}, Match{Kind: KindMeasurement, SensorType: "sensor"}, true},
		{"different sensors", Match{Kind: KindMeasurement, SensorType: "First"}, Match{Kind: KindMeasurement, SensorType: "Second"}, false},
	}
	// Every scalar filter uses exact comparison with an empty wildcard.
	for _, filter := range []struct {
		name  string
		set   func(*Match, string)
		thing bool
	}{
		{"device", func(m *Match, v string) { m.Device = v }, false},
		{"object", func(m *Match, v string) {
			m.Object = v
			if v != "" {
				m.Object = "urn:example:" + v
			}
		}, false},
		{"env", func(m *Match, v string) { m.Env = v }, false},
		{"port", func(m *Match, v string) { m.Port = v }, false},
		{"subType", func(m *Match, v string) { m.SubType = v }, true},
		{"relation", func(m *Match, v string) { m.Relation = v }, true},
		{"toType", func(m *Match, v string) { m.ToType = v }, true},
	} {
		a, b := m(""), m("")
		if filter.thing {
			a, b = thing(EventThingsV1Relations, "Source", "", false), thing(EventThingsV1Relations, "Source", "", false)
		}
		filter.set(&a, "first")
		filter.set(&b, "second")
		tests = append(tests, conflictCase{filter.name + " separate", a, b, false})
		filter.set(&b, "")
		tests = append(tests, conflictCase{filter.name + " wildcard", a, b, true})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := ruleSetFixture(tt.a, "FirstModel"), ruleSetFixture(tt.b, "SecondModel")
			a.ID, b.ID = "first-rule", "second-rule"
			for _, r := range []Rule{a, b} {
				if err := Validate([]Rule{r}); err != nil {
					t.Fatalf("fixture must be valid: %v", err)
				}
			}
			for _, order := range [][]Rule{{a, b}, {b, a}} {
				err := Validate(order)
				if (err != nil) != tt.conflict {
					t.Fatalf("conflict=%v: %v", tt.conflict, err)
				}
				if err != nil && (!strings.Contains(err.Error(), a.ID) || !strings.Contains(err.Error(), b.ID) || !strings.Contains(err.Error(), "entity 0")) {
					t.Fatalf("conflict lacks rule/entity context: %v", err)
				}
			}
		})
	}
}

func TestRuleSetPreservesAllDefinitions(t *testing.T) {
	a := ruleSetFixture(Match{Kind: KindMeasurement, Tenant: "a"}, "First")
	b := ruleSetFixture(Match{Kind: KindMeasurement, Tenant: "b"}, "Second")
	c := ruleSetFixture(Match{Kind: KindMeasurement, Tenant: "a"}, "Second")
	if err := Validate([]Rule{a, b}); err != nil {
		t.Fatal(err)
	}
	if err := Validate([]Rule{a, b, c}); err == nil {
		t.Fatal("intervening tenant must not hide earlier conflicting definition")
	}
}

func TestRuleSetRejectsLocalConflict(t *testing.T) {
	r := ruleSetFixture(Match{Kind: KindMeasurement, Tenant: "a"}, "First")
	e := r.Entities[0]
	e.Type = "Second"
	r.Entities = append(r.Entities, e)
	if err := Validate([]Rule{r}); err == nil {
		t.Fatal("local conflicting entity types must be rejected")
	}
}

func TestRuleSetTenantSelection(t *testing.T) {
	a := ruleSetFixture(Match{Kind: KindMeasurement, Tenant: "a"}, "First")
	b := ruleSetFixture(Match{Kind: KindMeasurement, Tenant: "b"}, "Second")
	if err := Validate([]Rule{a, b}); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine([]Rule{a, b})
	for _, tt := range []struct{ tenant, want string }{{"a", "First"}, {"b", "Second"}} {
		writes, err := engine.Transform(Measurement{DeviceID: "source", Tenant: tt.tenant})
		if err != nil || len(writes) != 1 || writes[0].TypeName != tt.want {
			t.Fatalf("tenant %s: %v, %v", tt.tenant, writes, err)
		}
	}
	if _, err := engine.Transform(Measurement{DeviceID: "source", Tenant: "c"}); err != ErrNoMatchingRule {
		t.Fatalf("unmatched tenant: %v", err)
	}
}
