package transform

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func geoRule(geometry map[string]any) Rule {
	return Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:G:x", Type: "G",
		Properties: []Property{{Target: "location", Type: TypeGeo, Source: Source{Const: geometry}}},
	}}}
}

func geoJSON(t, coords string) map[string]any {
	return map[string]any{"type": t, "coordinates": mustCoords(coords)}
}

func mustCoords(s string) any {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		panic(err)
	}
	return v
}

// Varje GeoJSON-typ via const: ger GeoProperty med samma value-form som
// biblioteket, för båda kinds (motorn är delad; här via mätvärden).
func TestGeoTakesAllGeometryTypes(t *testing.T) {
	is := is.New(t)

	cases := []struct {
		name string
		geo  map[string]any
		want string
	}{
		{"point", geoJSON("Point", `[17.5,62.3]`), `"type":"Point"`},
		{"linestring", geoJSON("LineString", `[[17.5,62.3],[17.6,62.4]]`), `"type":"LineString"`},
		{"polygon", geoJSON("Polygon", `[[[17.5,62.3],[17.6,62.3],[17.6,62.4],[17.5,62.3]]]`), `"type":"Polygon"`},
		{"multipoint", geoJSON("MultiPoint", `[[17.5,62.3],[17.6,62.4]]`), `"type":"MultiPoint"`},
		{"multilinestring", geoJSON("MultiLineString", `[[[17.5,62.3],[17.6,62.4]]]`), `"type":"MultiLineString"`},
		{"multipolygon", geoJSON("MultiPolygon", `[[[[17.5,62.3],[17.6,62.3],[17.6,62.4],[17.5,62.3]]]]`), `"type":"MultiPolygon"`},
	}

	for _, c := range cases {
		writes, err := NewEngine([]Rule{geoRule(c.geo)}).Transform(testMeasurement())
		is.NoErr(err)
		is.Equal(1, len(writes))
		b := marshalWrite(t, writes[0])
		is.True(strings.Contains(b, `"type":"GeoProperty"`))
		is.True(strings.Contains(b, c.want))
		is.True(strings.Contains(b, `17.5`))
		// Utan explicit observedAt-config: ingen tid läcker in.
		is.True(!strings.Contains(b, `observedAt`))
	}
}

// Ogiltig geometriform ger fel (inte tyst skip): okänd typ, tomma
// koordinater, icke-numeriska löv och saknade koordinater.
func TestGeoRejectsInvalidForm(t *testing.T) {
	is := is.New(t)

	invalid := []map[string]any{
		{"type": "Circle", "coordinates": []any{17.5, 62.3}},
		{"type": "Point", "coordinates": []any{}},
		{"type": "Point", "coordinates": []any{"öst", 62.3}},
		{"type": "Point"},
		{"coordinates": []any{17.5, 62.3}},
		{"type": "Polygon", "coordinates": []any{[]any{[]any{"a"}}}},
	}

	for _, geo := range invalid {
		_, err := NewEngine([]Rule{geoRule(geo)}).Transform(testMeasurement())
		is.True(err != nil)
	}
}

// latlon:-vägen är oförändrad (bibliotekets Point-form utan observedAt).
func TestGeoLatLonUnchanged(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()
	m.Lat, m.Lon, m.HasLatLon = 62.3, 17.5, true
	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:G:x", Type: "G",
		Properties: []Property{{Target: "location", Type: TypeGeo, Source: Source{LatLon: true}}},
	}}}
	writes, err := NewEngine([]Rule{r}).Transform(m)
	is.NoErr(err)
	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"type":"Point"`))
	is.True(!strings.Contains(b, `observedAt`))
}

// observedAt hedras på Boolean (genericProperty), Geo (GeoJSON-punkt) och
// Relationship (egen struct) — med identisk value-form som utan tid.
func TestObservedAtOnBooleanGeoRelationship(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()
	m.Records = []Record{{Name: "5700", Value: ptr(1)}}
	m.Lat, m.Lon, m.HasLatLon = 62.3, 17.5, true

	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:G:x", Type: "G",
		Properties: []Property{
			{Target: "on", Type: TypeBoolean, Source: Source{Resource: "5700"}, ObservedAt: ObservedAtHeader},
			{Target: "loc", Type: TypeGeo, Source: Source{LatLon: true}, ObservedAt: ObservedAtHeader},
			{Target: "ref", Type: TypeRelationship, Source: Source{Const: "urn:ngsi-ld:X:y"}, ObservedAt: ObservedAtHeader},
		},
	}}}
	writes, err := NewEngine([]Rule{r}).Transform(m)
	is.NoErr(err)
	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"on":{"type":"Property","value":true,"observedAt":"2026-01-01T12:00:00Z"}`))
	is.True(strings.Contains(b, `"loc":{"type":"GeoProperty","value":{"coordinates":[17.5,62.3],"type":"Point"},"observedAt":"2026-01-01T12:00:00Z"}`))
	is.True(strings.Contains(b, `"ref":{"type":"Relationship","object":"urn:ngsi-ld:X:y","observedAt":"2026-01-01T12:00:00Z"}`))
}

// Utan observedAt är Boolean/Relationship-output exakt som före (ingen
// observedAt-nyckel läcker in via generic-typerna).
func TestBooleanRelationshipUnchangedWithoutObservedAt(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()
	m.Records = []Record{{Name: "5700", Value: ptr(1)}}
	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:G:x", Type: "G",
		Properties: []Property{
			{Target: "on", Type: TypeBoolean, Source: Source{Resource: "5700"}},
			{Target: "ref", Type: TypeRelationship, Source: Source{Const: "urn:ngsi-ld:X:y"}},
		},
	}}}
	writes, err := NewEngine([]Rule{r}).Transform(m)
	is.NoErr(err)
	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"on":{"type":"Property","value":true}`))
	is.True(strings.Contains(b, `"ref":{"type":"Relationship","object":"urn:ngsi-ld:X:y"}`))
	is.True(!strings.Contains(b, `observedAt`))
}
