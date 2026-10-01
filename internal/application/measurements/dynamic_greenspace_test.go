package measurements

import (
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

const greenspaceRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3323", env: soil }
    entities:
      - id: "urn:ngsi-ld:GreenspaceRecord:{{deviceID}}"
        type: GreenspaceRecord
        properties:
          - target: soilMoisturePressure
            type: Number
            resource: "5700"
            unit: KPA
            observedAt: header
            observedBy: "urn:ngsi-ld:Device:{{deviceID}}"
            transform: [{ op: divide, value: 1000 }]
          - { target: location, type: Geo, latlon: true }
          - { target: dateObserved, type: DateTime, timestamp: true }
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3327", env: soil }
    entities:
      - id: "urn:ngsi-ld:GreenspaceRecord:{{deviceID}}"
        type: GreenspaceRecord
        properties:
          - target: soilMoistureEc
            type: Number
            resource: "5700"
            unit: MHO
            observedAt: header
            observedBy: "urn:ngsi-ld:Device:{{deviceID}}"
          - { target: location, type: Geo, latlon: true }
          - { target: dateObserved, type: DateTime, timestamp: true }
`

func greenspacePack(ts time.Time, object string, value float64) senml.Pack {
	return senml.Pack{
		{Name: "dev-1/0", StringValue: object, Time: float64(ts.Unix())},
		{Name: "dev-1/5700", Value: new(value)},
		{Name: "dev-1/env", StringValue: "soil"},
		{Name: "dev-1/lat", Value: new(62.36), Unit: senml.UnitLat},
		{Name: "dev-1/lon", Value: new(17.51), Unit: senml.UnitLon},
		{Name: "dev-1/tenant", StringValue: "default"},
	}
}

func TestGreenspacePressureIsConverted(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: greenspacePack(ts, "urn:oma:lwm2m:ext:3323", 50000), Timestamp: ts}

	b := transformFirst(t, greenspaceRules, msg)
	is.True(strings.Contains(b, `"soilMoisturePressure":{"type":"Property","value":50`))
	is.True(strings.Contains(b, `"unitCode":"KPA"`))
	is.True(strings.Contains(b, `"observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:dev-1"`))
}

func TestGreenspaceConductivity(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: greenspacePack(ts, "urn:oma:lwm2m:ext:3327", 12.5), Timestamp: ts}

	b := transformFirst(t, greenspaceRules, msg)
	is.True(strings.Contains(b, `"soilMoistureEc":{"type":"Property","value":12.5`))
	is.True(strings.Contains(b, `"unitCode":"MHO"`))
}
