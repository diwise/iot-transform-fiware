package transform

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestEntityReferenceBetweenEntities(t *testing.T) {
	is := is.New(t)

	rule := Rule{Match: Match{Kind: KindThing, Type: "Sewer"}, Entities: []Entity{
		{
			Name: "sewer", ID: "urn:ngsi-ld:Sewer:{{id}}", Type: "Sewer", Create: true,
			Properties: []Property{
				{Target: "name", Type: TypeText, Field: "name", ObservedAt: ObservedAtNone},
			},
		},
		{
			Name: "observation", ID: "urn:ngsi-ld:Observation:{{id}}", Type: "Observation",
			Properties: []Property{
				{Target: "refSewer", Type: TypeRelationship, Value: "{{entities.sewer.id}}"},
			},
		},
	}}

	writes, err := NewEngine([]Rule{rule}).Transform(relationThing())
	is.NoErr(err)
	is.Equal(len(writes), 2)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Sewer:s-1")
	is.True(writes[0].Create)
	is.Equal(writes[1].EntityID, "urn:ngsi-ld:Observation:s-1")

	b := marshalWrite(t, writes[1])
	is.True(strings.Contains(b, `"object":"urn:ngsi-ld:Sewer:s-1"`))
}

func TestEntityReferenceToLaterEntityFails(t *testing.T) {
	is := is.New(t)

	// Valideringen ska avvisa referenser till okända namn.
	rule := Rule{Match: Match{Kind: KindThing, Type: "Sewer"}, Entities: []Entity{
		{ID: "urn:ngsi-ld:Observation:x", Type: "Observation", Properties: []Property{
			{Target: "refSewer", Type: TypeRelationship, Value: "{{entities.missing.id}}"},
		}},
	}}

	is.True(Validate([]Rule{rule}) != nil)
}
