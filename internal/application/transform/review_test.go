package transform

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

// firstPresent ska falla tillbaka när ett field saknas, inte ge fel.
func TestFirstPresentFallsBackWhenFieldMissing(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindThing, Type: "Sewer"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:Observed:{{firstPresent(current.ref, id)}}", Type: "Observed",
	}}}

	writes, err := NewEngine([]Rule{rule}).Transform(relationThing())
	is.NoErr(err)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Observed:s-1")
}

func TestFirstPresentUsesFieldWhenPresent(t *testing.T) {
	is := is.New(t)

	th := relationThing()
	th.Fields["current"] = map[string]any{"ref": "dev-42"}

	rule := Rule{Match: Match{Kind: KindThing, Type: "Sewer"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:Observed:{{firstPresent(current.ref, id)}}", Type: "Observed",
	}}}

	writes, err := NewEngine([]Rule{rule}).Transform(th)
	is.NoErr(err)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Observed:dev-42")
}

func measurementWithResources(names ...string) Measurement {
	m := testMeasurement()
	m.Records = nil
	for _, n := range names {
		m.Records = append(m.Records, Record{Name: n, Value: ptr(1)})
	}
	return m
}

func resourceRule(resource string) Rule {
	r := validRule()
	r.Match = Match{Kind: KindMeasurement, Object: "urn:oma:lwm2m:ext:3303", Resource: resource}
	return r
}

func TestMatchOnResource(t *testing.T) {
	is := is.New(t)

	m := measurementWithResources("5700", "5500")

	writes, err := NewEngine([]Rule{resourceRule("5500")}).Transform(m)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	_, err = NewEngine([]Rule{resourceRule("9999")}).Transform(m)
	is.True(err == ErrNoMatchingRule)
}

func TestValidateRejectsKindSpecificMatchFields(t *testing.T) {
	is := is.New(t)

	measurementWithType := validRule()
	measurementWithType.Match.Type = "Container"
	is.True(Validate([]Rule{measurementWithType}) != nil)

	thingWithObject := Rule{Match: Match{Kind: KindThing, Object: "urn:oma:lwm2m:ext:3303"}, Entities: []Entity{validEntity()}}
	is.True(Validate([]Rule{thingWithObject}) != nil)

	// device är tillåtet för båda enligt planen (entiteten använder en
	// kind-neutral const-källa så att endast match-fältet prövas).
	thingWithDevice := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Device: "dev-1"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:Thing:x", Type: "Thing",
		Properties: []Property{{Target: "name", Type: TypeText, Source: Source{Const: "x"}}},
	}}}
	is.NoErr(Validate([]Rule{thingWithDevice}))
}

func TestValidateRejectsObservedByOnNonNumber(t *testing.T) {
	is := is.New(t)

	r := validRule()
	r.Entities[0].Properties[0].Type = TypeText
	r.Entities[0].Properties[0].ObservedBy = "urn:ngsi-ld:Device:x"
	is.True(Validate([]Rule{r}) != nil)
}

func TestConstIntConvertsToTextAndBoolean(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:T:x", Type: "T",
		Properties: []Property{
			{Target: "n", Type: TypeText, Source: Source{Const: 5}, ObservedAt: ObservedAtNone},
			{Target: "b", Type: TypeBoolean, Source: Source{Const: 1}, ObservedAt: ObservedAtNone},
		},
	}}}

	writes, err := NewEngine([]Rule{rule}).Transform(testMeasurement())
	is.NoErr(err)

	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"n":{"type":"Property","value":"5"`))
	is.True(strings.Contains(b, `"b":{"type":"Property","value":true`))
}
