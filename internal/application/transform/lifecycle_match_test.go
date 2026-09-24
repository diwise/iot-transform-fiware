package transform

import (
	"testing"

	"github.com/matryer/is"
)

func lifecycleInput(kind string) Thing {
	return Thing{
		ID: "room-a", Type: "room", Tenant: "t",
		TypeName: "room", NameOrID: "room-a",
		Event: EventThingsV1Lifecycle, Lifecycle: kind,
		Fields: map[string]any{},
	}
}

// Livscykelregler matchar endast lifecycle-händelser med samma typ;
// values-, relations- och thing.updated-indata når dem aldrig.
func TestLifecycleMatchIsolatesSources(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Lifecycle, Lifecycle: string(LifecycleDeleted)}}

	del := lifecycleInput(string(LifecycleDeleted))
	is.True(rule.Match.matches(del, del.MatchInfo()))

	createdRule := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Lifecycle, Lifecycle: string(LifecycleCreated)}}
	is.True(!createdRule.Match.matches(del, del.MatchInfo()))

	values := del
	values.Event = EventThingsV1Values
	values.Lifecycle = ""
	is.True(!rule.Match.matches(values, values.MatchInfo()))

	rel := del
	rel.Event = EventThingsV1Relations
	rel.Lifecycle = ""
	rel.Relation = "partOf"
	is.True(!rule.Match.matches(rel, rel.MatchInfo()))

	legacy := del
	legacy.Event = ""
	legacy.Lifecycle = ""
	is.True(!rule.Match.matches(legacy, legacy.MatchInfo()))
}

func TestLifecycleMatchValidation(t *testing.T) {
	is := is.New(t)

	// Okänd lifecyclytyp avvisas.
	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Lifecycle, Lifecycle: "archived"},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X"}}}
	is.True(Validate([]Rule{r}) != nil)

	// lifecycle utanför lifecycle-event avvisas.
	r = Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Lifecycle: string(LifecycleDeleted)},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X"}}}
	is.True(Validate([]Rule{r}) != nil)

	// lifecycle på mätvärdesregel avvisas.
	m := Rule{Match: Match{Kind: KindMeasurement, Object: "urn:oma:lwm2m:ext:3303"},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X"}}}
	m.Match.Lifecycle = string(LifecycleDeleted)
	is.True(Validate([]Rule{m}) != nil)

	// Kända typer accepteras.
	for _, kind := range []string{string(LifecycleCreated), string(LifecycleUpdated), string(LifecycleDeleted)} {
		ok := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Lifecycle, Lifecycle: kind},
			Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X"}}}
		is.NoErr(Validate([]Rule{ok}))
	}
}

func TestDeleteValidation(t *testing.T) {
	is := is.New(t)

	delMatch := Match{Kind: KindThing, Event: EventThingsV1Lifecycle, Lifecycle: string(LifecycleDeleted)}
	delEntity := Entity{ID: "urn:ngsi-ld:X:x", Type: "X", Delete: true}

	// delete med lifecycle: deleted är giltig.
	is.NoErr(Validate([]Rule{Rule{Match: delMatch, Entities: []Entity{delEntity}}}))

	// delete kräver exakt deleted — inte created, values eller tomt event.
	for _, mm := range []Match{
		{Kind: KindThing, Event: EventThingsV1Lifecycle, Lifecycle: string(LifecycleCreated)},
		{Kind: KindThing, Event: EventThingsV1Lifecycle, Lifecycle: string(LifecycleUpdated)},
		{Kind: KindThing, Event: EventThingsV1Values, Type: "room"},
		{Kind: KindThing},
	} {
		is.True(Validate([]Rule{Rule{Match: mm, Entities: []Entity{delEntity}}}) != nil)
	}

	// delete utesluter create/removeAttributes/properties och mätvärden.
	for _, e := range []Entity{
		{ID: "urn:ngsi-ld:X:x", Type: "X", Delete: true, Create: true},
		{ID: "urn:ngsi-ld:X:x", Type: "X", Delete: true, RemoveAttributes: []string{"a"}},
		{ID: "urn:ngsi-ld:X:x", Type: "X", Delete: true,
			Properties: []Property{{Target: "name", Type: TypeText, Source: Source{Const: "x"}}}},
	} {
		is.True(Validate([]Rule{Rule{Match: delMatch, Entities: []Entity{e}}}) != nil)
	}
	mr := Rule{Match: Match{Kind: KindMeasurement, Object: "urn:oma:lwm2m:ext:3303"},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X", Delete: true}}}
	is.True(Validate([]Rule{mr}) != nil)
}

// delete-entiteter bygger Delete-skrivningar utan properties.
func TestDeleteBuildsWrite(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Lifecycle, Lifecycle: string(LifecycleDeleted)},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:{{nameOrID}}", Type: "Room", Delete: true}}}
	is.NoErr(Validate([]Rule{rule}))

	th := lifecycleInput(string(LifecycleDeleted))
	th.NameOrID = "room-a"
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	is.NoErr(err)
	is.Equal(1, len(writes))
	is.True(writes[0].Delete)
	is.Equal("urn:ngsi-ld:X:room-a", writes[0].EntityID)
	is.Equal(0, len(writes[0].Props))
	is.Equal(0, len(writes[0].RemoveAttributes))
}
