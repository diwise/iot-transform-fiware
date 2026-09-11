package measurements

import (
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

const deviceRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3302" }
    entities:
      - id: "urn:ngsi-ld:Device:{{deviceID}}"
        type: Device
        properties:
          - { target: controlledProperty, type: TextArray, const: ["motion"], observedAt: none }
          - { target: dateLastValueReported, type: DateTime, timestamp: true }
          - { target: status, type: Text, resource: "5500", observedAt: none, transform: [{ op: boolToOnOff }] }
          - { target: location, type: Geo, latlon: true }
`

func TestDeviceIsModelConformant(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	presence := true
	msg := events.MessageAccepted{
		Pack_: senml.Pack{
			{Name: "dev-1/0", StringValue: "urn:oma:lwm2m:ext:3302", Time: float64(ts.Unix())},
			{Name: "dev-1/5500", BoolValue: &presence},
			{Name: "dev-1/lat", Value: ptr(62.36), Unit: senml.UnitLat},
			{Name: "dev-1/lon", Value: ptr(17.51), Unit: senml.UnitLon},
			{Name: "dev-1/tenant", StringValue: "default"},
		},
		Timestamp: ts,
	}

	b := transformFirst(t, deviceRules, msg)
	is.True(strings.Contains(b, `"controlledProperty":{"type":"Property","value":["motion"]`))
	is.True(strings.Contains(b, `"dateLastValueReported":{"type":"Property"`))
	is.True(strings.Contains(b, `"status":{"type":"Property","value":"on"`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
}
