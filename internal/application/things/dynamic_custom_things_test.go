package things

import (
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

const lifebuoyRules = `
rules:
  - match: { kind: thing, type: Lifebuoy }
    entities:
      - id: "urn:ngsi-ld:Lifebuoy:{{nameOrID}}"
        type: Lifebuoy
        properties:
          - { target: dateLastValueReported, type: DateTime, timestamp: true }
          - { target: status, type: Text, field: presence, observedAt: header, transform: [{ op: boolToOnOff }] }
          - { target: location, type: Geo, field: location }
`

const lifebuoyBody = `{"thing":{"id":"lb-1","type":"Lifebuoy","name":"lb1","presence":true,"location":{"latitude":1,"longitude":2},"observedAt":"2024-11-19T10:49:59Z","tenant":"default"},"tenant":"default","timestamp":"2024-11-19T10:50:00Z"}`

func TestLifebuoyIsMigrated(t *testing.T) {
	is := is.New(t)

	th, err := ThingFromJSON([]byte(lifebuoyBody))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(lifebuoyRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Lifebuoy:lb1")
	is.Equal(writes[0].TypeName, "Lifebuoy")

	b := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
	is.True(strings.Contains(b, `"status":{"type":"Property","value":"on"`))
	is.True(strings.Contains(b, `"dateLastValueReported":{"type":"Property"`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
}

const deskRules = `
rules:
  - match: { kind: thing, type: Desk }
    entities:
      - id: "urn:ngsi-ld:Device:{{nameOrID}}"
        type: Device
        properties:
          - { target: controlledProperty, type: TextArray, const: ["occupancy"], observedAt: none }
          - { target: dateLastValueReported, type: DateTime, timestamp: true }
          - { target: status, type: Text, field: presence, observedAt: header, transform: [{ op: boolToOnOff }] }
          - { target: location, type: Geo, field: location }
`

const deskBody = `{"thing":{"id":"desk-1","type":"Desk","name":"d1","presence":true,"location":{"latitude":1,"longitude":2},"observedAt":"2024-11-19T10:49:59Z","tenant":"default"},"tenant":"default","timestamp":"2024-11-19T10:50:00Z"}`

func TestDeskIsMigratedToDevice(t *testing.T) {
	is := is.New(t)

	th, err := ThingFromJSON([]byte(deskBody))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(deskRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Device:d1")
	is.Equal(writes[0].TypeName, "Device")

	b := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
	is.True(strings.Contains(b, `"controlledProperty":{"type":"Property","value":["occupancy"]`))
	is.True(strings.Contains(b, `"status":{"type":"Property","value":"on"`))
}

const pumpingRules = `
rules:
  - match: { kind: thing, type: PumpingStation }
    entities:
      - id: "urn:ngsi-ld:SewagePumpingStation:{{nameOrID}}"
        type: SewagePumpingStation
        properties:
          - { target: dateObserved, type: DateTime, field: observedAt }
          - { target: status, type: Text, field: pumpingObserved, observedAt: "field:pumpingObservedAt", transform: [{ op: boolToOnOff }] }
          - { target: location, type: Geo, field: location }
`

const pumpingBody = `{"thing":{"id":"pump-001","type":"PumpingStation","name":"p1","pumpingObserved":true,"pumpingObservedAt":"2025-01-15T07:47:38Z","location":{"latitude":0,"longitude":0},"observedAt":"2025-01-15T07:47:38Z","tenant":"default"},"tenant":"default","timestamp":"2025-01-15T07:47:40.360378603Z"}`

func TestPumpingStationIsMigrated(t *testing.T) {
	is := is.New(t)

	th, err := ThingFromJSON([]byte(pumpingBody))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(pumpingRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:SewagePumpingStation:p1")
	is.Equal(writes[0].TypeName, "SewagePumpingStation")

	b := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
	is.True(strings.Contains(b, `"status":{"type":"Property","value":"on","observedAt":"2025-01-15T07:47:38Z"`))
	is.True(strings.Contains(b, `"dateObserved":{"type":"Property"`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
}
