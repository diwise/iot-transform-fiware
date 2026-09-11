package things

import (
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

const roomRules = `
rules:
  - match: { kind: thing, type: Room }
    entities:
      - id: "urn:ngsi-ld:IndoorEnvironmentObserved:{{type}}:{{nameOrID}}"
        type: IndoorEnvironmentObserved
        properties:
          - { target: location, type: Geo, field: location }
          - { target: dateObserved, type: DateTime, timestamp: true }
          - { target: temperature, type: Number, field: temperature.v }
          - { target: humidity, type: Number, field: humidity }
          - { target: illuminance, type: Number, field: illuminance }
          - { target: CO2, type: Number, field: co2 }
          - { target: name, type: Text, field: name, observedAt: none }
          - { target: alternativeName, type: Text, field: alternativeName, observedAt: none }
`

func TestRoomMatchesDynamicEngine(t *testing.T) {
	is := is.New(t)

	temperature := 21.5
	r := Room{
		Thing: Thing{
			ID: "room-1", Type: "Room", Name: "Rum 1", AlternativeName: "R1",
			Location: testLocation(), ObservedAt: testTime(), Tenant: "default",
		},
		Temperature: Measurement{Value: &temperature, Timestamp: testTime()},
		Humidity:    40,
		Illuminance: 300,
		CO2:         450,
	}

	static := TransformRoom(r)[0]

	th, err := ThingFromJSON(envelope(t, testTime(), r))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(roomRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	is.Equal(
		marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props),
		marshalEntity(t, static.EntityID, static.TypeName, static.Props),
	)
}
