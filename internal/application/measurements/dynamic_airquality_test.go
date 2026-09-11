package measurements

import (
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

const airQualityRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3428" }
    entities:
      - id: "urn:ngsi-ld:AirQualityObserved:{{deviceID}}"
        type: AirQualityObserved
        properties:
          - { target: co2, type: Number, resource: "17", observedAt: header }
          - { target: pm10, type: Number, resource: "1", observedAt: header }
          - { target: pm25, type: Number, resource: "3", observedAt: header }
          - { target: pm1, type: Number, resource: "5", observedAt: header }
          - { target: no2, type: Number, resource: "15", observedAt: header }
          - { target: no, type: Number, resource: "19", observedAt: header }
          - { target: location, type: Geo, latlon: true, required: true }
          - { target: dateObserved, type: DateTime, timestamp: true, required: true }
`

func airQualityPack(ts time.Time) senml.Pack {
	return senml.Pack{
		{Name: "dev-1/0", StringValue: "urn:oma:lwm2m:ext:3428", Time: float64(ts.Unix())},
		{Name: "dev-1/17", Value: ptr(400)},
		{Name: "dev-1/1", Value: ptr(12)},
		{Name: "dev-1/3", Value: ptr(8)},
		{Name: "dev-1/5", Value: ptr(4)},
		{Name: "dev-1/15", Value: ptr(30)},
		{Name: "dev-1/19", Value: ptr(10)},
		{Name: "dev-1/lat", Value: ptr(62.36), Unit: senml.UnitLat},
		{Name: "dev-1/lon", Value: ptr(17.51), Unit: senml.UnitLon},
		{Name: "dev-1/tenant", StringValue: "default"},
	}
}

func TestAirQualityObservedIsModelConformant(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: airQualityPack(ts), Timestamp: ts}

	b := transformFirst(t, airQualityRules, msg)
	is.True(strings.Contains(b, `"co2":{"type":"Property","value":400`))
	is.True(strings.Contains(b, `"pm10":{"type":"Property","value":12`))
	is.True(strings.Contains(b, `"pm25":{"type":"Property","value":8`))
	is.True(strings.Contains(b, `"pm1":{"type":"Property","value":4`))
	is.True(strings.Contains(b, `"no2":{"type":"Property","value":30`))
	is.True(strings.Contains(b, `"no":{"type":"Property","value":10`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
	is.True(strings.Contains(b, `"dateObserved":{"type":"Property"`))
	is.True(!strings.Contains(b, `"CO2"`))
	is.True(!strings.Contains(b, `"PM10"`))
}
