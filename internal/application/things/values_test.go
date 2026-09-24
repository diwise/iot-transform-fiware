package things

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// Golden trådform (K1-kontraktet fält för fält): rum med tre individuella
// temperaturer + medel, en ogiltig fukt som ska filtreras, GeoJSON-plats.
const valuesRoomGolden = `{
  "schema":"v1","eventId":"v1/drill/room-a/drill-m2","tenant":"drill",
  "thingId":"room-a","thingType":"room","thingTypeVersion":"v1",
  "configRevision":2,"publishedAt":"2026-09-21T08:13:32Z",
  "thing":{"thingId":"room-a","name":"Rum A","category":"room",
    "location":{"type":"Point","coordinates":[18.0,59.3]}},
  "values":[
    {"schema":"v1","thingId":"room-a","tenant":"drill","propertyId":"temperature.window",
     "value":18.0,"quality":"ok","unit":"Cel","source":"drill-s1/0/3303/5700",
     "messageId":"drill-m2","observedAt":"2026-09-21T08:11:32Z",
     "receivedAt":"2026-09-21T08:13:32Z","computedAt":"2026-09-21T08:13:32Z",
     "publishedAt":"2026-09-21T08:13:32Z","configRevision":2,
     "eventId":"v1/drill/room-a/drill-m2/temperature.window"},
    {"schema":"v1","thingId":"room-a","tenant":"drill","propertyId":"temperature.average",
     "value":21.0,"quality":"ok","unit":"Cel","source":"drill-s1/0/3303/5700,drill-s1/1/3303/5700,drill-s1/2/3303/5700",
     "messageId":"drill-m2","observedAt":"2026-09-21T08:13:32Z",
     "receivedAt":"2026-09-21T08:13:32Z","computedAt":"2026-09-21T08:13:32Z",
     "publishedAt":"2026-09-21T08:13:32Z","configRevision":2,
     "eventId":"v1/drill/room-a/drill-m2/temperature.average"},
    {"schema":"v1","thingId":"room-a","tenant":"drill","propertyId":"humidity",
     "value":99.0,"quality":"invalid","unit":"%RH","source":"drill-s1/3304/5700",
     "messageId":"drill-m2","observedAt":"2026-09-21T08:13:32Z",
     "receivedAt":"2026-09-21T08:13:32Z","computedAt":"2026-09-21T08:13:32Z",
     "publishedAt":"2026-09-21T08:13:32Z","configRevision":2,
     "eventId":"v1/drill/room-a/drill-m2/humidity"}
  ]
}`

func TestThingValuesFromJSONBuildsEngineInput(t *testing.T) {
	th, err := ThingValuesFromJSON([]byte(valuesRoomGolden))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if th.ID != "room-a" || th.Type != "room" || th.Tenant != "drill" {
		t.Fatalf("identitet=%+v", th)
	}
	if th.NameOrID != "Rum:A" {
		t.Fatalf("NameOrID=%q, want sanitized Rum:A", th.NameOrID)
	}
	if th.TypeName != "room" {
		t.Fatalf("TypeName=%q, want room", th.TypeName)
	}
	if th.Event != transform.EventThingsV1Values {
		t.Fatalf("Event=%q, want things.v1.values", th.Event)
	}
	wantObserved := time.Date(2026, 9, 21, 8, 13, 32, 0, time.UTC)
	if !th.ObservedAt.Equal(wantObserved) {
		t.Fatalf("ObservedAt=%v, want max %v", th.ObservedAt, wantObserved)
	}
	vals, ok := th.Fields["values"].(map[string]any)
	if !ok {
		t.Fatalf("values saknas i Fields: %+v", th.Fields)
	}
	avg, ok := vals["temperature.average"].(map[string]any)
	if !ok || avg["value"] != 21.0 || avg["quality"] != "ok" {
		t.Fatalf("average=%+v", vals["temperature.average"])
	}
	if _, ok := vals["humidity"]; ok {
		t.Fatal("invalid-kvalitet får aldrig nå motorn (ersätter aldrig aktuellt)")
	}
	loc, ok := th.Fields["location"].(map[string]any)
	if !ok || loc["latitude"] != 59.3 || loc["longitude"] != 18.0 {
		t.Fatalf("location=%+v, want lat/lon-mapp", th.Fields["location"])
	}
}

func TestThingValuesFromJSONSkipsNilValuedEntry(t *testing.T) {
	body := `{"thingId":"x","tenant":"t","thingType":"room",
	  "thing":{"thingId":"x"},
	  "values":[{"thingId":"x","tenant":"t","propertyId":"temperature.average",
	    "value":null,"quality":"ok","observedAt":"2026-09-21T08:13:32Z"}]}`
	th, err := ThingValuesFromJSON([]byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	vals := th.Fields["values"].(map[string]any)
	if _, ok := vals["temperature.average"]; ok {
		t.Fatal("värdelös post ska utelämnas (required hoppar entiteten)")
	}
}

func TestThingValuesFromJSONWastebinIdentity(t *testing.T) {
	body := `{"schema":"v1","eventId":"v1/t/bin-42/m1","tenant":"t",
	  "thingId":"bin-42","thingType":"wastebin","thingTypeVersion":"v1",
	  "configRevision":2,"publishedAt":"2026-09-21T08:13:32Z",
	  "thing":{"thingId":"bin-42","name":"Soptunna 42","alternativeName":"BinA",
	    "subType":"WasteContainer","category":"wastebin",
	    "location":{"type":"Point","coordinates":[18.0,59.3]}},
	  "values":[{"thingId":"bin-42","tenant":"t","propertyId":"fillRate",
	    "value":33.0,"quality":"uncertain","unit":"pct","source":"s/3330/5700",
	    "observedAt":"2026-09-21T08:13:32Z"}]}`
	th, err := ThingValuesFromJSON([]byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if th.NameOrID != "BinA" {
		t.Fatalf("NameOrID=%q, want BinA (alternativeName vinner)", th.NameOrID)
	}
	if th.TypeName != "WasteContainer" {
		t.Fatalf("TypeName=%q, want WasteContainer (subType vinner)", th.TypeName)
	}
	vals := th.Fields["values"].(map[string]any)
	if vals["fillRate"].(map[string]any)["quality"] != "uncertain" {
		t.Fatal("uncertain ska passera till motorn (flaggas vidare)")
	}
}

func TestThingValuesFromJSONRejectsMissingIdentity(t *testing.T) {
	for _, body := range []string{
		`{"thingId":"","tenant":"t","values":[]}`,
		`{ojsan`,
	} {
		if _, err := ThingValuesFromJSON([]byte(body)); err == nil {
			t.Fatalf("body %q ska nekas permanent", body)
		}
	}
	// Tom tenant är skip, inte strukturfel (handler-grind som thing.updated).
	th, err := ThingValuesFromJSON([]byte(`{"thingId":"x","tenant":"","thingType":"room","values":[]}`))
	if err != nil {
		t.Fatalf("tom tenant ska passera adaptern: %v", err)
	}
	if th.Tenant != "" {
		t.Fatalf("tenant=%q, want tom", th.Tenant)
	}
}

func TestThingValuesFromJSONSkipsNonPointLocation(t *testing.T) {
	body := `{"thingId":"p","tenant":"t","thingType":"room",
	  "thing":{"thingId":"p","location":{"type":"Polygon","coordinates":[[[18.0,59.3]]]}},
	  "values":[]}`
	th, err := ThingValuesFromJSON([]byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, ok := th.Fields["location"]; ok {
		t.Fatal("polygon får inte bli Geo-punkt (required hoppar entiteten i stället)")
	}
	// Polygonen bevaras rått som fields["geometry"] för Geo-regler.
	geom, ok := th.Fields["geometry"].(map[string]any)
	if !ok {
		t.Fatalf("geometry=%+v, want rå GeoJSON-mapp", th.Fields["geometry"])
	}
	if geom["type"] != "Polygon" {
		t.Fatalf("geometry.type=%v, want Polygon", geom["type"])
	}
}

func TestThingValuesFromJSONExposesMetadata(t *testing.T) {
	body := `{"thingId":"p","tenant":"t","thingType":"room",
	  "thing":{"thingId":"p","metadata":{"floor":"2","zone":"A"}},
	  "values":[]}`
	th, err := ThingValuesFromJSON([]byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	meta, ok := th.Fields["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata=%+v, want mapp", th.Fields["metadata"])
	}
	if meta["floor"] != "2" || meta["zone"] != "A" {
		t.Fatalf("metadata=%+v, want floor/zone", meta)
	}
	// field:metadata.<nyckel> fungerar för Text via motorn.
	d := NewDynamicThingTransformer([]transform.Rule{{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "room"},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:X:x", Type: "X",
			Properties: []transform.Property{{
				Target: "floor", Type: transform.TypeText,
				Source: transform.Source{Field: "metadata.floor"},
			}},
		}},
	}})
	writes, err := d.Transform(th)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(writes) != 1 {
		t.Fatalf("writes=%d, want 1", len(writes))
	}
}

func TestThingValuesFromJSONOmitsMissingGeometryAndMetadata(t *testing.T) {
	th, err := ThingValuesFromJSON([]byte(`{"thingId":"p","tenant":"t","thingType":"room","values":[]}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, ok := th.Fields["geometry"]; ok {
		t.Fatal("saknad plats ska inte ge geometry-nyckel")
	}
	if _, ok := th.Fields["metadata"]; ok {
		t.Fatal("saknad metadata ska inte ge metadata-nyckel")
	}
	if _, ok := th.Fields["location"]; ok {
		t.Fatal("saknad plats ska inte ge location-nyckel")
	}
}

// End-to-end genom motorn med v2-regel: växthus → AgriGreenhouse.
// Samma isoleringsgaranti som rum: gamla regler (utan event-tagg)
// aktiveras aldrig av versionerade event.
func TestThingValuesEndToEndGreenhouse(t *testing.T) {
	body := `{"schema":"v1","eventId":"v1/t/gh-1/m1","tenant":"t",
	  "thingId":"gh-1","thingType":"greenhouse","thingTypeVersion":"v1",
	  "configRevision":3,"publishedAt":"2026-09-21T08:13:32Z",
	  "thing":{"thingId":"gh-1","name":"Växthus 1","alternativeName":"Greenhouse 1",
	    "category":"greenhouse"},
	  "values":[
	    {"thingId":"gh-1","tenant":"t","propertyId":"co2",
	     "value":800.0,"quality":"ok","unit":"ppm","source":"s/3428/17",
	     "observedAt":"2026-09-21T08:13:32Z"},
	    {"thingId":"gh-1","tenant":"t","propertyId":"humidity",
	     "value":65.0,"quality":"ok","unit":"%RH","source":"s/3304/5700",
	     "observedAt":"2026-09-21T08:13:32Z"}]}`
	th, err := ThingValuesFromJSON([]byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if th.NameOrID != "Greenhouse:1" {
		t.Fatalf("NameOrID=%q, want sanitized Greenhouse:1", th.NameOrID)
	}
	d := NewDynamicThingTransformer([]transform.Rule{
		{
			Match: transform.Match{Kind: transform.KindThing, Type: "Greenhouse"},
			Entities: []transform.Entity{{
				ID: "urn:ngsi-ld:OLD:{{nameOrID}}", Type: "OLD",
			}},
		},
		{
			Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "greenhouse"},
			Entities: []transform.Entity{{
				ID:      "urn:ngsi-ld:AgriGreenhouse:{{nameOrID}}",
				Type:    "AgriGreenhouse",
				Context: "https://example.invalid/agri-context.jsonld",
				Properties: []transform.Property{
					{Target: "co2", Type: transform.TypeNumber, Field: "values.co2.value", ObservedAt: `field:values.co2.observedAt`, Required: true},
					{Target: "relativeHumidity", Type: transform.TypeNumber, Field: "values.humidity.value", ObservedAt: `field:values.humidity.observedAt`},
				},
			}},
		},
	})
	writes, err := d.Transform(th)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(writes) != 1 {
		t.Fatalf("writes=%d, want 1", len(writes))
	}
	w := writes[0]
	if w.EntityID != "urn:ngsi-ld:AgriGreenhouse:Greenhouse:1" {
		t.Fatalf("id=%q", w.EntityID)
	}
	if w.TypeName != "AgriGreenhouse" {
		t.Fatalf("type=%q", w.TypeName)
	}
}
func TestThingValuesEndToEndRoom(t *testing.T) {
	th, err := ThingValuesFromJSON([]byte(valuesRoomGolden))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	d := NewDynamicThingTransformer([]transform.Rule{
		{
			Match: transform.Match{Kind: transform.KindThing, Type: "Room"},
			Entities: []transform.Entity{{
				ID: "urn:ngsi-ld:OLD:{{nameOrID}}", Type: "OLD",
			}},
		},
		{
			Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "room"},
			Entities: []transform.Entity{{
				ID:   "urn:ngsi-ld:IndoorEnvironmentObserved:{{type}}:{{nameOrID}}",
				Type: "IndoorEnvironmentObserved",
				Properties: []transform.Property{
					{Target: "location", Type: transform.TypeGeo, Field: "location", Required: true},
					{Target: "temperature", Type: transform.TypeNumber, Field: `values."temperature.average".value`, ObservedAt: `field:values."temperature.average".observedAt`, Required: true},
				},
			}},
		},
	})
	writes, err := d.Transform(th)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(writes) != 1 {
		t.Fatalf("writes=%d, want 1", len(writes))
	}
	w := writes[0]
	if w.EntityID != "urn:ngsi-ld:IndoorEnvironmentObserved:room:Rum:A" {
		t.Fatalf("id=%q", w.EntityID)
	}
	if w.TypeName != "IndoorEnvironmentObserved" {
		t.Fatalf("type=%q", w.TypeName)
	}
	raw, _ := json.Marshal(w.Props)
	t.Logf("props: %s", raw)
}
