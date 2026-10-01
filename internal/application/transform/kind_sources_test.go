package transform

import (
	"testing"

	"github.com/matryer/is"
)

func kindRule(kind Kind, src Source) Rule {
	propertyType := TypeNumber
	if src.LatLon {
		propertyType = TypeGeo
	}
	if src.Timestamp {
		propertyType = TypeDateTime
	}
	if src.Const != nil || src.Value != "" {
		propertyType = TypeText
	}
	match := Match{Kind: kind}
	if kind == KindThing {
		match.Event = EventThingsV1Values
	}
	return Rule{Match: match, Entities: []Entity{{
		ID: "urn:ngsi-ld:X:x", Type: "X",
		Properties: []Property{{Target: "v", Type: propertyType, Source: src}},
	}}}
}

// Mätvärdeskällor avvisas på thing-regler (de skippas i dag tyst i
// Thing.Resolve): resource, object, env och name — även i Derive.Of och
// subproperties.
func TestThingRulesRejectMeasurementSources(t *testing.T) {
	is := is.New(t)

	for _, src := range []Source{
		{Resource: "5700"},
		{Object: "urn:oma:lwm2m:ext:3303", Resource: "5700"},
		{Env: "indoors", Resource: "5700"},
		{Name: "tenant"},
	} {
		is.True(Validate([]Rule{kindRule(KindThing, src)}) != nil)
	}

	// Rekursivt: Derive.Of och subproperties går via in.Resolve.
	derive := kindRule(KindThing, Source{})
	derive.Entities[0].Properties[0].Derive = &Derive{Fn: FnSum, Of: []Source{{Resource: "5700"}}}
	is.True(Validate([]Rule{derive}) != nil)

	sub := kindRule(KindThing, Source{Const: "x"})
	sub.Entities[0].Properties[0].Type = TypeStructured
	sub.Entities[0].Properties[0].Source = Source{}
	sub.Entities[0].Properties[0].Subproperties = []Property{
		{Target: "s", Type: TypeNumber, Resource: "5700"},
	}
	is.True(Validate([]Rule{sub}) != nil)

	// Kind-neutrala källor passerar på thing-regler.
	for _, src := range []Source{
		{Field: "values.temperature.value"},
		{Ref: "values.temperature.value"},
		{Const: "x"},
		{Value: "{{id}}"},
		{LatLon: true},
		{Timestamp: true},
	} {
		is.NoErr(Validate([]Rule{kindRule(KindThing, src)}))
	}
}

// Sakällor avvisas på mätvärdesregler: field och ref — även i Derive.Of
// och subproperties. Geo + field:/ref: på mätvärdesregler avvisas därmed
// också (posterna bär aldrig GeoJSON).
func TestMeasurementRulesRejectThingSources(t *testing.T) {
	is := is.New(t)

	for _, src := range []Source{
		{Field: "values.temperature.value"},
		{Ref: "values.temperature.value"},
	} {
		is.True(Validate([]Rule{kindRule(KindMeasurement, src)}) != nil)
	}

	geoField := kindRule(KindMeasurement, Source{Field: "geometry"})
	geoField.Entities[0].Properties[0].Type = TypeGeo
	is.True(Validate([]Rule{geoField}) != nil)

	derive := kindRule(KindMeasurement, Source{})
	derive.Entities[0].Properties[0].Derive = &Derive{Fn: FnSum, Of: []Source{{Field: "x"}}}
	is.True(Validate([]Rule{derive}) != nil)

	sub := kindRule(KindMeasurement, Source{Const: "x"})
	sub.Entities[0].Properties[0].Type = TypeStructured
	sub.Entities[0].Properties[0].Source = Source{}
	sub.Entities[0].Properties[0].Subproperties = []Property{
		{Target: "s", Type: TypeNumber, Ref: "x"},
	}
	is.True(Validate([]Rule{sub}) != nil)

	// Kind-neutrala källor passerar på mätvärdesregler.
	for _, src := range []Source{
		{Resource: "5700"},
		{Name: "tenant"},
		{Const: "x"},
		{Value: "{{deviceID}}"},
		{LatLon: true},
		{Timestamp: true},
	} {
		is.NoErr(Validate([]Rule{kindRule(KindMeasurement, src)}))
	}
}

// LanguageMap kräver field eller const: const: fungerar för båda kinds,
// field: endast för things.
func TestLanguageMapSourceKinds(t *testing.T) {
	is := is.New(t)

	lm := func(kind Kind, src Source) Rule {
		match := Match{Kind: kind}
		if kind == KindThing {
			match.Event = EventThingsV1Values
		}
		return Rule{Match: match, Entities: []Entity{{
			ID: "urn:ngsi-ld:X:x", Type: "X",
			Properties: []Property{{Target: "name", Type: TypeLanguageMap, Source: src}},
		}}}
	}

	is.NoErr(Validate([]Rule{lm(KindThing, Source{Field: "metadata"})}))
	is.NoErr(Validate([]Rule{lm(KindThing, Source{Const: map[string]any{"sv": "Hej"}})}))
	is.NoErr(Validate([]Rule{lm(KindMeasurement, Source{Const: map[string]any{"sv": "Hej"}})}))
	is.True(Validate([]Rule{lm(KindMeasurement, Source{Field: "metadata"})}) != nil)
}
