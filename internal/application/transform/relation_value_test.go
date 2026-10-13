package transform

import (
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
)

func relationThing() Thing {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	return Thing{
		ID: "s-1", Type: "Sewer", TypeName: "Sewer", Tenant: "default",
		ObservedAt: ts,
		Fields: map[string]any{
			"refDevices": []any{map[string]any{"deviceID": "dev-9"}},
		},
	}
}

func relationRule(properties ...Property) Rule {
	return Rule{Match: Match{Kind: KindThing, Type: "Sewer"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:Sewer:{{id}}", Type: "Sewer",
		Properties: properties,
	}}}
}

func TestRelationshipValueTemplate(t *testing.T) {
	is := is.New(t)

	rule := relationRule(
		Property{Target: "refDevice", Type: TypeRelationship, Value: "urn:ngsi-ld:Device:{{id}}"},
	)

	writes, err := NewEngine([]Rule{rule}).Transform(relationThing())
	is.NoErr(err)

	is.True(strings.Contains(marshalWrite(t, writes[0]), `"object":"urn:ngsi-ld:Device:s-1"`))
}

func TestRelationshipConst(t *testing.T) {
	is := is.New(t)

	rule := relationRule(
		Property{Target: "refArea", Type: TypeRelationship, Const: "urn:ngsi-ld:Area:7"},
	)

	writes, err := NewEngine([]Rule{rule}).Transform(relationThing())
	is.NoErr(err)

	is.True(strings.Contains(marshalWrite(t, writes[0]), `"object":"urn:ngsi-ld:Area:7"`))
}

func TestRelationshipFieldWithPrefix(t *testing.T) {
	is := is.New(t)

	rule := relationRule(
		Property{Target: "refDevice", Type: TypeRelationship,
			Field: "refDevices[0].deviceID", Prefix: "urn:ngsi-ld:Device:"},
	)

	writes, err := NewEngine([]Rule{rule}).Transform(relationThing())
	is.NoErr(err)

	is.True(strings.Contains(marshalWrite(t, writes[0]), `"object":"urn:ngsi-ld:Device:dev-9"`))
}

func TestTextWithPrefix(t *testing.T) {
	is := is.New(t)

	rule := relationRule(
		Property{Target: "source", Type: TypeText, ObservedAt: ObservedAtNone,
			Field: "refDevices[0].deviceID", Prefix: "urn:ngsi-ld:Device:"},
	)

	writes, err := NewEngine([]Rule{rule}).Transform(relationThing())
	is.NoErr(err)

	is.True(strings.Contains(marshalWrite(t, writes[0]), `"source":{"type":"Property","value":"urn:ngsi-ld:Device:dev-9"`))
}
