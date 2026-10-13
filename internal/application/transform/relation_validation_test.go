package transform

import (
	"testing"

	"github.com/matryer/is"
)

func relationRef(id, value string) Entity {
	return Entity{ID: id, Type: "Observer", Properties: []Property{{
		Target: "refTarget", Type: TypeRelationship, Value: value,
	}}}
}

func TestValidateRejectsForwardReferenceToNonCreatedEntity(t *testing.T) {
	is := is.New(t)

	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "Sewer"}, Entities: []Entity{
		relationRef("urn:ngsi-ld:Observer:a", "{{entities.target.id}}"),
		{Name: "target", ID: "urn:ngsi-ld:Target:b", Type: "Target"},
	}}

	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsUnknownReferenceInRelationValue(t *testing.T) {
	is := is.New(t)

	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "Sewer"}, Entities: []Entity{
		relationRef("urn:ngsi-ld:Observer:a", "{{entities.missing.id}}"),
	}}

	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsEmptyFieldPath(t *testing.T) {
	is := is.New(t)

	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "Sewer"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:Observer:a", Type: "Observer",
		Properties: []Property{{Target: "x", Type: TypeText, Value: "{{field:}}"}},
	}}}

	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateAllowsDynamicFieldInFirstPresent(t *testing.T) {
	is := is.New(t)

	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "Sewer"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:Observer:a", Type: "Observer",
		Properties: []Property{{Target: "x", Type: TypeText, Value: "{{firstPresent(bogus, id)}}"}},
	}}}

	is.NoErr(Validate([]Rule{r}))
}

func TestValidateAcceptsFieldAndFirstPresent(t *testing.T) {
	is := is.New(t)

	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "Sewer"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:Observer:{{firstPresent(current.ref, id)}}", Type: "Observer",
		Properties: []Property{{
			Target: "refTarget", Type: TypeRelationship,
			Value: "urn:ngsi-ld:Device:{{safeURI(field:current.ref)}}",
		}},
	}}}

	is.NoErr(Validate([]Rule{r}))
}

func TestValidateRejectsDuplicateEntityNames(t *testing.T) {
	is := is.New(t)

	r := Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "Sewer"}, Entities: []Entity{
		{Name: "same", ID: "urn:a", Type: "A"},
		{Name: "same", ID: "urn:b", Type: "B"},
	}}

	is.True(Validate([]Rule{r}) != nil)
}
