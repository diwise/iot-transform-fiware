package transform

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
)

func inverseRule() Rule {
	return Rule{ID: "inverse", Revision: 2,
		Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "belongsTo", Type: "sample", ToType: "group"},
		Entities: []Entity{{ID: "urn:example:Group:{{relation.to.thingId}}", Type: "Group",
			Properties: []Property{{Target: "members", Type: TypeRelationship,
				Value: "urn:example:Sample:{{relation.from.thingId}}", Aggregate: &RelationAggregate{Direction: "reverse"}}}}}}
}

func inverseLink(tenant, from, to string) relations.Link {
	return relations.Link{Tenant: tenant, FromID: from, ToID: to, Relation: "belongsTo", FromKind: "sample", ToKind: "group"}
}

func inverseObject(t *testing.T, w EntityWrite) []string {
	t.Helper()
	e, err := BuildEntity(w, "")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Members struct {
			Type   string   `json:"type"`
			Object []string `json:"object"`
		} `json:"members"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Members.Type != "Relationship" {
		t.Fatalf("not a relationship: %s", raw)
	}
	return doc.Members.Object
}

func TestRelationAggregateFourMembersDeduplicatedSorted(t *testing.T) {
	r := inverseRule()
	if err := Validate([]Rule{r}); err != nil {
		t.Fatal(err)
	}
	e := NewEngine([]Rule{r})
	links := []relations.Link{inverseLink("t", "4", "g"), inverseLink("t", "2", "g"), inverseLink("t", "1", "g"), inverseLink("t", "3", "g"), inverseLink("t", "2", "g")}
	snaps := relations.SnapshotsForRebuild(links, "t", "")
	writes, err := e.AggregateRelations(relations.Snapshot{Tenant: "t", Groups: snaps})
	if err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 {
		t.Fatalf("writes=%d", len(writes))
	}
	want := []string{"urn:example:Sample:1", "urn:example:Sample:2", "urn:example:Sample:3", "urn:example:Sample:4"}
	if got := inverseObject(t, writes[0]); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	// Normal event transformation never emits a one-member approximation.
	stateless, err := e.Transform(RelationInput(links[0], false))
	if err != nil || len(stateless) != 0 {
		t.Fatalf("stateless=%+v err=%v", stateless, err)
	}
}

func TestRelationAggregateRemoveMoveEmptyAndDelete(t *testing.T) {
	e := NewEngine([]Rule{inverseRule()})
	old, remaining, moved := inverseLink("t", "1", "a"), inverseLink("t", "2", "a"), inverseLink("t", "1", "b")
	snap := relations.Snapshot{Tenant: "t", Relation: "belongsTo", FromID: "1",
		Outgoing: []relations.Link{moved}, Removed: []relations.Link{old},
		ReverseNew: map[string][]relations.Link{"b": {moved}}, ReversePrevious: map[string][]relations.Link{"a": {remaining}}}
	writes, err := e.AggregateRelations(snap)
	if err != nil || len(writes) != 2 {
		t.Fatalf("move=%+v err=%v", writes, err)
	}
	byID := map[string]EntityWrite{}
	for _, w := range writes {
		byID[w.EntityID] = w
	}
	if got := inverseObject(t, byID["urn:example:Group:a"]); !reflect.DeepEqual(got, []string{"urn:example:Sample:2"}) {
		t.Fatal(got)
	}
	if got := inverseObject(t, byID["urn:example:Group:b"]); !reflect.DeepEqual(got, []string{"urn:example:Sample:1"}) {
		t.Fatal(got)
	}
	snap.Outgoing = nil
	snap.ReverseNew = nil
	snap.ReversePrevious["a"] = []relations.Link{}
	writes, err = e.AggregateRelations(snap)
	if err != nil || len(writes) != 1 || !reflect.DeepEqual(writes[0].RemoveAttributes, []string{"members"}) {
		t.Fatalf("empty=%+v %v", writes, err)
	}
	snap.EndpointDeleted = "a"
	writes, err = e.AggregateRelations(snap)
	if err != nil || len(writes) != 0 {
		t.Fatalf("deleted owner must not be recreated: %+v %v", writes, err)
	}
}

func TestRelationAggregateTenantAndKindIsolation(t *testing.T) {
	e := NewEngine([]Rule{inverseRule()})
	l := inverseLink("t", "1", "a")
	snap := relations.Snapshot{Tenant: "t", Relation: l.Relation, Outgoing: []relations.Link{l}, ReverseNew: map[string][]relations.Link{"a": {l, inverseLink("foreign", "2", "a")}}}
	if _, err := e.AggregateRelations(snap); err == nil {
		t.Fatal("foreign member accepted")
	}
	snap.ReverseNew["a"] = []relations.Link{l, {Tenant: "t", Relation: l.Relation, FromID: "other", ToID: "a", FromKind: "unrelated", ToKind: "group"}}
	writes, err := e.AggregateRelations(snap)
	if err != nil || len(writes) != 1 {
		t.Fatal(writes, err)
	}
	if got := inverseObject(t, writes[0]); !reflect.DeepEqual(got, []string{"urn:example:Sample:1"}) {
		t.Fatal(got)
	}
}

func TestRelationAggregateValidationAndOwnership(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Rule)
	}{
		{"direction", func(r *Rule) { r.Entities[0].Properties[0].Aggregate.Direction = "forward" }},
		{"array", func(r *Rule) { r.Entities[0].Properties[0].Array = true }},
		{"kind", func(r *Rule) { r.Entities[0].Properties[0].Type = TypeText }},
		{"subtype", func(r *Rule) { r.Match.SubType = "special" }},
		{"remove", func(r *Rule) { r.Match.RelationRemoved = true }},
		{"name identity", func(r *Rule) { r.Entities[0].ID = "urn:example:Group:{{relation.to.name}}" }},
		{"missing type", func(r *Rule) { r.Match.ToType = "" }},
		{"source", func(r *Rule) { r.Entities[0].Properties[0].Const = map[string]any{"bad": true} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := inverseRule()
			tc.mutate(&r)
			if err := Validate([]Rule{r}); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	r := inverseRule()
	for _, event := range []string{EventThingsV1Values, EventThingsV1Lifecycle} {
		other := Rule{Match: Match{Kind: KindThing, Event: event, Type: "group"}, Entities: []Entity{{ID: "urn:example:Group:{{id}}", Type: "Group", Properties: []Property{{Target: "members", Type: TypeRelationship, Const: "urn:other:x"}}}}}
		if event == EventThingsV1Lifecycle {
			other.Match.Lifecycle = "updated"
		}
		if err := Validate([]Rule{r, other}); err == nil || !strings.Contains(err.Error(), "owned") {
			t.Fatalf("owner not protected: %v", err)
		}
		other.Entities[0].Properties[0].Target = "unrelated"
		if err := Validate([]Rule{r, other}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRelationAggregateVersionFilterStillClearsAffectedOwner(t *testing.T) {
	r := inverseRule()
	r.Match.ThingTypeVersion = "1"
	e := NewEngine([]Rule{r})
	l := inverseLink("t", "1", "a")
	l.FromKindVersion = "2"
	snap := relations.Snapshot{Tenant: "t", Relation: l.Relation, Outgoing: []relations.Link{l}, ReverseNew: map[string][]relations.Link{"a": {l}}}
	writes, err := e.AggregateRelations(snap)
	if err != nil || len(writes) != 1 || !reflect.DeepEqual(writes[0].RemoveAttributes, []string{"members"}) {
		t.Fatal(writes, err)
	}
}
