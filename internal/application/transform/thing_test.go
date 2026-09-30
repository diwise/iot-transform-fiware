package transform

import (
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
)

func testThing() Thing {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	return Thing{
		ID: "id-1", Type: "Container", TypeName: "Container", Tenant: "default",
		ObservedAt: ts,
		Fields: map[string]any{
			"percent":    42.5,
			"observedAt": ts.Format(time.RFC3339),
			"location":   map[string]any{"latitude": 62.36, "longitude": 17.51},
			"current":    map[string]any{"ref": "urn:ngsi-ld:Device:dev-7"},
			"refDevices": []any{map[string]any{"deviceID": "dev-7"}},
		},
	}
}

func TestThingFieldAndRefSources(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindThing, Type: "Container"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:{{type}}:{{id}}", Type: "{{type}}",
		Properties: []Property{
			{Target: "fillingLevel", Type: TypeNumber, Source: Source{Field: "percent"}, ObservedAt: ObservedAtHeader},
			{Target: "location", Type: TypeGeo, Source: Source{Field: "location"}},
			{Target: "refDevice", Type: TypeRelationship, Source: Source{Ref: "current.ref"}},
			{Target: "device", Type: TypeText, Source: Source{Field: "refDevices[0].deviceID"}},
		},
	}}}

	writes, err := NewEngine([]Rule{rule}).Transform(testThing())
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Container:id-1")
	is.Equal(writes[0].TypeName, "Container")

	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"value":42.5`))
	is.True(strings.Contains(b, `"coordinates":[17.51,62.36]`))
	is.True(strings.Contains(b, `urn:ngsi-ld:Device:dev-7`))
	is.True(strings.Contains(b, `"value":"dev-7"`))
}

func TestThingDoesNotMatchMeasurementRule(t *testing.T) {
	is := is.New(t)

	rule := measurementRule("urn:ngsi-ld:Thing:x", "", "", "")
	_, err := NewEngine([]Rule{rule}).Transform(testThing())
	is.True(err == ErrNoMatchingRule)
}

func TestMeasurementDoesNotMatchThingRule(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindThing, Type: "Container"}, Entities: []Entity{validEntity()}}
	_, err := NewEngine([]Rule{rule}).Transform(testMeasurement())
	is.True(err == ErrNoMatchingRule)
}

func thingRule(id, typ, subType, tenant string) Rule {
	return Rule{
		Match:    Match{Kind: KindThing, Type: typ, SubType: subType, Tenant: tenant},
		Entities: []Entity{{ID: id, Type: "Thing"}},
	}
}

func TestThingPrecedenceSubTypeOverTypeOverTenant(t *testing.T) {
	is := is.New(t)

	th := testThing()
	th.SubType = "WasteContainer"

	byTenant := thingRule("urn:ngsi-ld:Thing:tenant", "", "", "default")
	bySubType := thingRule("urn:ngsi-ld:Thing:sub", "", "WasteContainer", "")
	byType := thingRule("urn:ngsi-ld:Thing:type", "Container", "", "")

	writes, err := NewEngine([]Rule{byTenant, bySubType, byType}).Transform(th)
	is.NoErr(err)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Thing:sub")
}

func TestThingTieBreaksByConfigOrder(t *testing.T) {
	is := is.New(t)

	first := thingRule("urn:ngsi-ld:Thing:first", "Container", "", "")
	second := thingRule("urn:ngsi-ld:Thing:second", "Container", "", "")

	writes, err := NewEngine([]Rule{first, second}).Transform(testThing())
	is.NoErr(err)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Thing:first")
}

func TestThingTenantMustMatch(t *testing.T) {
	is := is.New(t)

	rule := thingRule("urn:ngsi-ld:Thing:x", "Container", "", "other")
	_, err := NewEngine([]Rule{rule}).Transform(testThing())
	is.True(err == ErrNoMatchingRule)
}

func TestThingUuidVariable(t *testing.T) {
	is := is.New(t)

	th := testThing()
	th.ID = "urn:ngsi-ld:Thing:abc-123"

	rule := Rule{Match: Match{Kind: KindThing, Type: "Container"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:Thing:{{uuid}}", Type: "Thing",
		Properties: []Property{{Target: "name", Type: TypeText, Source: Source{Field: "percent"}}},
	}}}

	writes, err := NewEngine([]Rule{rule}).Transform(th)
	is.NoErr(err)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Thing:abc-123")
}
