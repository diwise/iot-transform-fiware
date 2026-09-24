package transform

import (
	"testing"

	"github.com/matryer/is"
)

func relationInput() Thing {
	return Thing{
		ID: "tank-1", Type: "tank", Tenant: "t",
		TypeName: "tank", NameOrID: "tank-1",
		Event: EventThingsV1Relations, Relation: "partOf",
		Fields: map[string]any{},
	}
}

func removedRelationInput() Thing {
	th := relationInput()
	th.RelationRemoved = true
	return th
}

// Relationsregler matchar endast relationshändelser med samma
// relationstyp; values- och thing.updated-indata når dem aldrig.
func TestRelationMatchIsolatesSources(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "partOf", Type: "tank"}}

	rel := relationInput()
	is.True(rule.Match.matches(rel, rel.MatchInfo()))

	values := rel
	values.Event = EventThingsV1Values
	values.Relation = ""
	is.True(!rule.Match.matches(values, values.MatchInfo()))

	legacy := rel
	legacy.Event = ""
	legacy.Relation = ""
	is.True(!rule.Match.matches(legacy, legacy.MatchInfo()))

	// Set-regel matchar inte removed-händelser och tvärtom.
	removedRule := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "partOf", RelationRemoved: true, Type: "tank"}}
	is.True(!removedRule.Match.matches(rel, rel.MatchInfo()))
	is.True(removedRule.Match.matches(removedRelationInput(), removedRelationInput().MatchInfo()))
	is.True(!rule.Match.matches(removedRelationInput(), removedRelationInput().MatchInfo()))
}

// Relationsregler kräver typavgränsning: utan match.type/subType skulle
// regeln träffa alla sakers länkar, även från producenter utan typfält.
func TestRelationMatchRequiresType(t *testing.T) {
	is := is.New(t)

	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "partOf"},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X"}}}
	is.True(Validate([]Rule{r}) != nil)

	r.Match.Type = "tank"
	is.NoErr(Validate([]Rule{r}))

	r.Match.Type = ""
	r.Match.SubType = "Tank"
	is.NoErr(Validate([]Rule{r}))
}

func TestRelationMatchValidation(t *testing.T) {
	is := is.New(t)

	// relationRemoved utan relation avvisas.
	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, RelationRemoved: true, Type: "tank"},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X"}}}
	is.True(Validate([]Rule{r}) != nil)

	// relation på mätvärdesregel avvisas.
	m := Rule{Match: Match{Kind: KindMeasurement, Object: "urn:oma:lwm2m:ext:3303"},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X"}}}
	m.Match.Relation = "partOf"
	is.True(Validate([]Rule{m}) != nil)

	// removeAttributes endast thing-regler, med namn, utan create/properties.
	e := Entity{ID: "urn:ngsi-ld:X:x", Type: "X", RemoveAttributes: []string{"refParent"}}
	is.NoErr(Validate([]Rule{Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "partOf", Type: "tank"}, Entities: []Entity{e}}}))

	bad := Entity{ID: "urn:ngsi-ld:X:x", Type: "X", RemoveAttributes: []string{""}}
	is.True(Validate([]Rule{Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "partOf", Type: "tank"}, Entities: []Entity{bad}}}) != nil)

	badCreate := Entity{ID: "urn:ngsi-ld:X:x", Type: "X", Create: true, RemoveAttributes: []string{"refParent"}}
	is.True(Validate([]Rule{Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "partOf", Type: "tank"}, Entities: []Entity{badCreate}}}) != nil)

	badProps := Entity{ID: "urn:ngsi-ld:X:x", Type: "X", RemoveAttributes: []string{"refParent"},
		Properties: []Property{{Target: "name", Type: TypeText, Source: Source{Const: "x"}}}}
	is.True(Validate([]Rule{Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "partOf", Type: "tank"}, Entities: []Entity{badProps}}}) != nil)

	mr := Rule{Match: Match{Kind: KindMeasurement, Object: "urn:oma:lwm2m:ext:3303"},
		Entities: []Entity{{ID: "urn:ngsi-ld:X:x", Type: "X", RemoveAttributes: []string{"refParent"}}}}
	is.True(Validate([]Rule{mr}) != nil)
}

// removeAttributes-entiteter bygger RemoveAttributes-skrivningar utan
// properties; vanliga entiteter påverkas inte.
func TestRemoveAttributesBuildsWrite(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Relations, Relation: "partOf", RelationRemoved: true, Type: "tank"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:AgriGreenhouse:{{field:relation.to.nameOrID}}", Type: "AgriGreenhouse",
			RemoveAttributes: []string{"hasWaterQualityObserved"},
		}}}
	is.NoErr(Validate([]Rule{rule}))

	th := removedRelationInput()
	th.Fields["relation"] = map[string]any{"to": map[string]any{"nameOrID": "greenhouse-a"}}
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	is.NoErr(err)
	is.Equal(1, len(writes))
	is.Equal("urn:ngsi-ld:AgriGreenhouse:greenhouse-a", writes[0].EntityID)
	is.Equal([]string{"hasWaterQualityObserved"}, writes[0].RemoveAttributes)
	is.Equal(0, len(writes[0].Props))
}
