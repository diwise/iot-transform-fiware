package things

import (
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

const sewerRules = `
rules:
  - match: { kind: thing, type: Sewer }
    entities:
      - id: "urn:ngsi-ld:{{type}}:{{nameOrID}}"
        type: "{{type}}"
        properties:
          - { target: location, type: Geo, field: location }
          - { target: name, type: Text, field: name, observedAt: none }
          - { target: description, type: Text, field: description, observedAt: none }
          - { target: level, type: Number, field: "measured.level", observedAt: "field:measured.observedAt" }
          - { target: percent, type: Number, field: "measured.percent", observedAt: "field:measured.observedAt" }
          - { target: dateObserved, type: DateTime, field: observedAt }
          - { target: status, type: Text, field: overflowObserved, observedAt: "field:overflowObservedAt" }
          - { target: refDevice, type: Relationship, field: "refDevices[0].deviceID", prefix: "urn:ngsi-ld:Device:" }
          - { target: source, type: Text, field: "refDevices[0].deviceID", prefix: "urn:ngsi-ld:Device:", observedAt: none }
`

const sewerBody = `{"thing":{"id":"25ba0559-3d49-4853-a537-3bbf7d2ae777","type":"Sewer","subType":"CombinedSewerOverflow","name":"05","description":null,"location":{"latitude":62.395275,"longitude":17.462769},"refDevices":[{"deviceID":"eef259d2-0cf9-5fa3-82e6-e8f95159e931"}],"observedAt":"2024-11-27T06:12:58Z","tenant":"default","currentLevel":0,"percent":0,"overflowObserved":false,"overflowObservedAt":null,"overflowDuration":null,"overflowCumulativeTime":0},"tenant":"default","timestamp":"2024-12-09T09:48:39.31863409Z"}`

func TestSewerRefDeviceAndSource(t *testing.T) {
	is := is.New(t)

	th, err := ThingFromJSON([]byte(sewerBody))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(sewerRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:CombinedSewerOverflow:05")
	is.Equal(writes[0].TypeName, "CombinedSewerOverflow")

	b := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
	is.True(strings.Contains(b, `"refDevice":{"type":"Relationship","object":"urn:ngsi-ld:Device:eef259d2-0cf9-5fa3-82e6-e8f95159e931"`))
	is.True(strings.Contains(b, `"source":{"type":"Property","value":"urn:ngsi-ld:Device:eef259d2-0cf9-5fa3-82e6-e8f95159e931"`))
	is.True(strings.Contains(b, `"name":{"type":"Property","value":"05"`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
}
