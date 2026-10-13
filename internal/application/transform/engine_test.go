package transform

import (
	"testing"
	"time"

	"github.com/matryer/is"
)

func measurementRule(id, device, sensorType, object string) Rule {
	r := validRule()
	r.Match = Match{Kind: KindMeasurement, Device: device, SensorType: sensorType, Object: object}
	r.Entities[0].ID = id
	r.Entities[0].Properties = []Property{{Target: "label", Type: TypeText, Const: "match-test"}}
	return r
}

func testMeasurement() Measurement {
	return Measurement{
		DeviceID:   "dev-1",
		SensorType: "temperature",
		Object:     "urn:oma:lwm2m:ext:3303",
		Env:        "air",
		Tenant:     "default",
		Timestamp:  time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
}

func TestEnginePrefersDeviceOverSensorTypeOverObject(t *testing.T) {
	is := is.New(t)

	byObject := measurementRule("urn:ngsi-ld:Thing:object", "", "", "urn:oma:lwm2m:ext:3303")
	bySensor := measurementRule("urn:ngsi-ld:Thing:sensor", "", "temperature", "")
	byDevice := measurementRule("urn:ngsi-ld:Thing:device", "dev-1", "", "")

	e := NewEngine([]Rule{byObject, bySensor, byDevice})

	writes, err := e.Transform(testMeasurement())
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Thing:device")
}

func TestEngineTieBreaksByConfigOrder(t *testing.T) {
	is := is.New(t)

	first := measurementRule("urn:ngsi-ld:Thing:first", "", "", "urn:oma:lwm2m:ext:3303")
	second := measurementRule("urn:ngsi-ld:Thing:second", "", "", "urn:oma:lwm2m:ext:3303")

	e := NewEngine([]Rule{first, second})

	writes, err := e.Transform(testMeasurement())
	is.NoErr(err)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Thing:first")
}

func TestEngineNoMatchReturnsSentinel(t *testing.T) {
	is := is.New(t)

	e := NewEngine([]Rule{measurementRule("urn:ngsi-ld:Thing:x", "other-device", "", "")})

	_, err := e.Transform(testMeasurement())
	is.True(err != nil)
	is.True(err == ErrNoMatchingRule)
}

func TestEngineRendersTemplatesAndEntityReferences(t *testing.T) {
	is := is.New(t)

	r := Rule{
		Match: Match{Kind: KindMeasurement},
		Entities: []Entity{
			{Name: "device", ID: "urn:ngsi-ld:Device:{{deviceID}}", Type: "Device", Properties: []Property{{Target: "label", Type: TypeText, Const: "device"}}},
			{Name: "observed", ID: "urn:ngsi-ld:TemperatureObserved:{{entities.device.id}}", Type: "TemperatureObserved", Properties: []Property{{Target: "label", Type: TypeText, Const: "observed"}}},
		},
	}

	e := NewEngine([]Rule{r})
	writes, err := e.Transform(testMeasurement())
	is.NoErr(err)
	is.Equal(len(writes), 2)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Device:dev-1")
	is.Equal(writes[1].EntityID, "urn:ngsi-ld:TemperatureObserved:urn:ngsi-ld:Device:dev-1")
}

func TestEngineSafeURI(t *testing.T) {
	is := is.New(t)

	r := Rule{
		Match:    Match{Kind: KindMeasurement},
		Entities: []Entity{{ID: "urn:ngsi-ld:Thing:{{safeURI(deviceID)}}", Type: "Thing", Properties: []Property{{Target: "label", Type: TypeText, Const: "identity-test"}}}},
	}

	m := testMeasurement()
	m.DeviceID = "a b/c"

	e := NewEngine([]Rule{r})
	writes, err := e.Transform(m)
	is.NoErr(err)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Thing:a%20b%2Fc")
}
