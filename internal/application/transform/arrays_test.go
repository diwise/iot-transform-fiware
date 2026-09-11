package transform

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func arrayThing() Thing {
	th := testThing()
	th.Fields["category"] = []any{"strand", "bad"}
	th.Fields["values"] = []any{1.0, 2.0}
	th.Fields["refSeeAlso"] = []any{"urn:ngsi-ld:Beach:a", "urn:ngsi-ld:Beach:b"}
	th.Fields["address"] = map[string]any{"street": "Main", "country": "SE"}
	return th
}

func arrayRule(properties ...Property) Rule {
	return Rule{Match: Match{Kind: KindThing, Type: "Container"}, Entities: []Entity{{
		ID: "urn:ngsi-ld:{{type}}:{{nameOrID}}", Type: "{{type}}",
		Properties: properties,
	}}}
}

func TestTextAndNumberArray(t *testing.T) {
	is := is.New(t)

	rule := arrayRule(
		Property{Target: "category", Type: TypeTextArray, Source: Source{Field: "category"}},
		Property{Target: "values", Type: TypeNumberArray, Source: Source{Field: "values"}, Unit: "Cel"},
	)

	writes, err := NewEngine([]Rule{rule}).Transform(arrayThing())
	is.NoErr(err)

	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"category":{"type":"Property","value":["strand","bad"]`))
	is.True(strings.Contains(b, `"value":[1,2]`))
	is.True(strings.Contains(b, `"unitCode":"Cel"`))
}

func TestRelationshipArray(t *testing.T) {
	is := is.New(t)

	rule := arrayRule(Property{Target: "refSeeAlso", Type: TypeRelationship, Source: Source{Field: "refSeeAlso"}})

	writes, err := NewEngine([]Rule{rule}).Transform(arrayThing())
	is.NoErr(err)

	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"urn:ngsi-ld:Beach:a"`))
	is.True(strings.Contains(b, `"urn:ngsi-ld:Beach:b"`))
}

func TestStructured(t *testing.T) {
	is := is.New(t)

	rule := arrayRule(Property{
		Target: "address", Type: TypeStructured,
		Subproperties: []Property{
			{Target: "streetAddress", Type: TypeText, Source: Source{Field: "address.street"}},
			{Target: "addressCountry", Type: TypeText, Source: Source{Field: "address.country"}},
			{Target: "kind", Type: TypeText, Source: Source{Const: "postal"}},
		},
	})

	writes, err := NewEngine([]Rule{rule}).Transform(arrayThing())
	is.NoErr(err)

	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"streetAddress":"Main"`))
	is.True(strings.Contains(b, `"addressCountry":"SE"`))
	is.True(strings.Contains(b, `"kind":"postal"`))
}

func TestStructuredMissingSubpropertyIsSkipped(t *testing.T) {
	is := is.New(t)

	rule := arrayRule(Property{
		Target: "address", Type: TypeStructured,
		Subproperties: []Property{
			{Target: "streetAddress", Type: TypeText, Source: Source{Field: "address.street"}},
			{Target: "missing", Type: TypeText, Source: Source{Field: "nope"}},
		},
	})

	writes, err := NewEngine([]Rule{rule}).Transform(arrayThing())
	is.NoErr(err)

	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"streetAddress":"Main"`))
	is.True(!strings.Contains(b, `"missing"`))
}

func TestNestedStructuredIsRejected(t *testing.T) {
	is := is.New(t)

	rule := arrayRule(Property{
		Target: "outer", Type: TypeStructured,
		Subproperties: []Property{{
			Target: "inner", Type: TypeStructured,
			Subproperties: []Property{{Target: "x", Type: TypeText, Source: Source{Field: "a"}}},
		}},
	})

	is.True(Validate([]Rule{rule}) != nil)
}
