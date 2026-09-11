package measurements

import (
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

const indoorRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3303", env: indoors }
    entities:
      - id: "urn:ngsi-ld:IndoorEnvironmentObserved:{{deviceID}}"
        type: IndoorEnvironmentObserved
        properties:
          - { target: temperature, type: Number, resource: "5700", observedAt: header }
          - { target: location, type: Geo, latlon: true, required: true }
          - { target: dateObserved, type: DateTime, timestamp: true, required: true }
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3304", env: indoors }
    entities:
      - id: "urn:ngsi-ld:IndoorEnvironmentObserved:{{deviceID}}"
        type: IndoorEnvironmentObserved
        properties:
          - { target: relativeHumidity, type: Number, resource: "5700", observedAt: header }
          - { target: location, type: Geo, latlon: true, required: true }
          - { target: dateObserved, type: DateTime, timestamp: true, required: true }
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3301", env: indoors }
    entities:
      - id: "urn:ngsi-ld:IndoorEnvironmentObserved:{{deviceID}}"
        type: IndoorEnvironmentObserved
        properties:
          - { target: illuminance, type: Number, resource: "5700", observedAt: header }
          - { target: location, type: Geo, latlon: true, required: true }
          - { target: dateObserved, type: DateTime, timestamp: true, required: true }
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3434", env: indoors }
    entities:
      - id: "urn:ngsi-ld:IndoorEnvironmentObserved:{{deviceID}}"
        type: IndoorEnvironmentObserved
        properties:
          - { target: peopleCount, type: Number, resource: "1", observedAt: header }
          - { target: location, type: Geo, latlon: true, required: true }
          - { target: dateObserved, type: DateTime, timestamp: true, required: true }
`

func indoorPack(ts time.Time, object string, resource string, value float64) senml.Pack {
	return senml.Pack{
		{Name: "dev-1/0", StringValue: object, Time: float64(ts.Unix())},
		{Name: "dev-1/" + resource, Value: ptr(value)},
		{Name: "dev-1/env", StringValue: "indoors"},
		{Name: "dev-1/lat", Value: ptr(62.36), Unit: senml.UnitLat},
		{Name: "dev-1/lon", Value: ptr(17.51), Unit: senml.UnitLon},
		{Name: "dev-1/tenant", StringValue: "default"},
	}
}

func TestIndoorTemperatureIsModelConformant(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: indoorPack(ts, "urn:oma:lwm2m:ext:3303", "5700", 22.5), Timestamp: ts}

	b := transformFirst(t, indoorRules, msg)
	is.True(strings.Contains(b, `"temperature":{"type":"Property","value":22.5`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
	is.True(strings.Contains(b, `"dateObserved":{"type":"Property"`))
}

func TestIndoorHumidityUsesRelativeHumidity(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: indoorPack(ts, "urn:oma:lwm2m:ext:3304", "5700", 40), Timestamp: ts}

	b := transformFirst(t, indoorRules, msg)
	is.True(strings.Contains(b, `"relativeHumidity":{"type":"Property","value":40`))
	is.True(!strings.Contains(b, `"humidity"`))
}
