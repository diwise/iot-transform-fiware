package transform

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func languageMapRule(src Source) Rule {
	return Rule{Match: Match{Kind: KindThing, Event: EventThingsV1Values}, Entities: []Entity{{
		ID: "urn:ngsi-ld:POI:x", Type: "PointOfInterest",
		Properties: []Property{{Target: "name", Type: TypeLanguageMap, Source: src}},
	}}}
}

func languageMapThing() Thing {
	return Thing{
		ID: "poi-1", Type: "pointofinterest", Tenant: "t",
		TypeName: "pointofinterest",
		Event:    EventThingsV1Values,
		Fields: map[string]any{
			"metadata": map[string]any{"sv": "Strand", "en": "Beach"},
			"empty":    map[string]any{},
			"count":    3.0,
		},
	}
}

// LanguageMap från field: (strängmapp) och const: (mapp) ger
// LanguageProperty; tom mapp skippas (required ger fel).
func TestLanguageMapBuildsProperty(t *testing.T) {
	is := is.New(t)

	writes, err := NewEngine([]Rule{languageMapRule(Source{Field: "metadata"})}).Transform(languageMapThing())
	is.NoErr(err)
	is.Equal(1, len(writes))
	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"name":{"type":"LanguageProperty","languageMap":{"en":"Beach","sv":"Strand"}}`))

	writes, err = NewEngine([]Rule{languageMapRule(Source{Const: map[string]any{"sv": "Hej"}})}).Transform(languageMapThing())
	is.NoErr(err)
	is.Equal(1, len(writes))
	is.True(strings.Contains(marshalWrite(t, writes[0]), `"languageMap":{"sv":"Hej"}`))

	// Tom mapp = saknad källa: tom props-lista utan required (befintlig
	// skip-semantik), fel med required.
	writes, err = NewEngine([]Rule{languageMapRule(Source{Field: "empty"})}).Transform(languageMapThing())
	is.NoErr(err)
	is.Equal(1, len(writes))
	is.Equal(0, len(writes[0].Props))

	required := languageMapRule(Source{Field: "empty"})
	required.Entities[0].Properties[0].Required = true
	writes, err = NewEngine([]Rule{required}).Transform(languageMapThing())
	is.NoErr(err)
	is.Equal(0, len(writes))

	// Fel typ ger fel (inte tyst skip): tal och sträng är inga mappar.
	for _, src := range []Source{{Field: "count"}, {Const: "hej"}} {
		_, err = NewEngine([]Rule{languageMapRule(src)}).Transform(languageMapThing())
		is.True(err != nil)
	}
}

// transform/derive avvisas på LanguageMap; andra källor än field/const
// avvisas — för båda kinds där källan tillåter.
func TestLanguageMapValidation(t *testing.T) {
	is := is.New(t)

	tr := languageMapRule(Source{Field: "metadata"})
	tr.Entities[0].Properties[0].Transform = []Transform{{Op: OpMultiply, Value: new(float64(2))}}
	is.True(Validate([]Rule{tr}) != nil)

	dr := languageMapRule(Source{Field: "metadata"})
	dr.Entities[0].Properties[0].Derive = &Derive{Fn: FnFirstPresent, Of: []Source{{Field: "metadata"}}}
	is.True(Validate([]Rule{dr}) != nil)

	for _, src := range []Source{{Ref: "metadata"}, {Value: "x"}, {Resource: "5700"}} {
		is.True(Validate([]Rule{languageMapRule(src)}) != nil)
	}

	is.NoErr(Validate([]Rule{languageMapRule(Source{Field: "metadata"})}))
	is.NoErr(Validate([]Rule{languageMapRule(Source{Const: map[string]any{"sv": "Hej"}})}))
}
