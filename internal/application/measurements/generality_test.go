package measurements

import (
	"testing"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

// Generalitetsbevis: samma logiska entitet (flerspråkigt namn + polygon +
// relation med observedAt) mappad från (a) ett mätvärdespack och (b) ett
// things-v2-värdesevent ger identisk NGSI-LD-JSON. Låser att båda källorna
// når alla nya konstruktioner (LanguageMap, Geo-GeoJSON, observedAt på
// Relationship).
func TestGeneralityGolden(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	measurementRulesYAML := `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3303" }
    entities:
      - id: "urn:ngsi-ld:PointOfInterest:poi-1"
        type: PointOfInterest
        properties:
          - target: name
            type: LanguageMap
            const: { sv: "Strand", en: "Beach" }
          - target: location
            type: Geo
            const: { type: Polygon, coordinates: [[[17.5, 62.3], [17.6, 62.3], [17.6, 62.4], [17.5, 62.3]]] }
          - target: refSeeAlso
            type: Relationship
            resource: link
            observedAt: header
`
	thingRulesYAML := `
rules:
  - match: { kind: thing, event: things.v1.values, type: poi }
    entities:
      - id: "urn:ngsi-ld:PointOfInterest:poi-1"
        type: PointOfInterest
        properties:
          - { target: name, type: LanguageMap, field: metadata }
          - { target: location, type: Geo, field: geometry }
          - { target: refSeeAlso, type: Relationship, field: values.link.source, observedAt: "field:values.link.observedAt" }
`

	// (a) Mätvärdessidan: språk/geometri som const, relation från strängpost.
	pack := senml.Pack{
		{Name: "dev-1/0", StringValue: "urn:oma:lwm2m:ext:3303", Time: float64(ts.Unix())},
		{Name: "dev-1/link", StringValue: "urn:ngsi-ld:Device:dev-1"},
		{Name: "dev-1/tenant", StringValue: "t"},
	}
	fromMeasurements := transformFirst(t, measurementRulesYAML,
		events.MessageAccepted{Pack_: pack, Timestamp: ts})

	// (b) Things-sidan: språk från metadata, geometri från plats, relation
	// från värdets source-sträng med värdets tid.
	body := `{"schema":"v1","eventId":"v1/t/poi-1/m1","tenant":"t",
	  "thingId":"poi-1","thingType":"poi","thingTypeVersion":"v1",
	  "configRevision":1,"publishedAt":"2026-09-17T12:00:00Z",
	  "thing":{"thingId":"poi-1","name":"Stranden",
	    "metadata":{"sv":"Strand","en":"Beach"},
	    "location":{"type":"Polygon","coordinates":[[[17.5,62.3],[17.6,62.3],[17.6,62.4],[17.5,62.3]]]}},
	  "values":[
	    {"thingId":"poi-1","tenant":"t","propertyId":"link",
	     "value":1.0,"quality":"ok","source":"urn:ngsi-ld:Device:dev-1",
	     "observedAt":"2026-09-17T12:00:00Z"}]}`
	th, err := appthings.ThingValuesFromJSON([]byte(body))
	if err != nil {
		t.Fatalf("ThingValuesFromJSON: %v", err)
	}
	thingRules, err := config.ParseModel([]byte(thingRulesYAML))
	if err != nil {
		t.Fatalf("ParseModel: %v", err)
	}
	writes, err := transform.NewEngine(thingRules).Transform(th)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	if len(writes) != 1 {
		t.Fatalf("expected exactly one write, got %d", len(writes))
	}
	fromThings := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)

	is.Equal(fromMeasurements, fromThings)
}
