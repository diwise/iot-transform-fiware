package things

import (
	"strings"
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
          - { target: location, type: Geo, field: location, required: true }
          - { target: dateObserved, type: DateTime, timestamp: true, required: true }
          - { target: temperature, type: Number, field: temperature.v }
          - { target: relativeHumidity, type: Number, field: humidity }
          - { target: illuminance, type: Number, field: illuminance }
          - { target: co2, type: Number, field: co2 }
          - { target: name, type: Text, field: name, observedAt: none }
          - { target: alternateName, type: Text, field: alternativeName, observedAt: none }
`

func TestRoomIsModelConformant(t *testing.T) {
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

	th, err := ThingFromJSON(envelope(t, testTime(), r))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(roomRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	b := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
	is.True(strings.Contains(b, `"temperature":{"type":"Property","value":21.5`))
	is.True(strings.Contains(b, `"relativeHumidity":{"type":"Property","value":40`))
	is.True(strings.Contains(b, `"illuminance":{"type":"Property","value":300`))
	is.True(strings.Contains(b, `"co2":{"type":"Property","value":450`))
	is.True(strings.Contains(b, `"alternateName":{"type":"Property","value":"R1"`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
	is.True(strings.Contains(b, `"dateObserved":{"type":"Property"`))
	is.True(!strings.Contains(b, `"CO2"`))
	is.True(!strings.Contains(b, `"humidity"`))
	is.True(!strings.Contains(b, `"alternativeName"`))
}

func TestRoomWithoutRequiredLocationIsSkipped(t *testing.T) {
	is := is.New(t)

	body := []byte(`{"thing":{"id":"room-1","type":"Room","name":"n","temperature":{"v":21.5},"humidity":40,"illuminance":300,"co2":450,"tenant":"default"},"tenant":"default","timestamp":"2024-01-01T00:00:00Z"}`)
	th, err := ThingFromJSON(body)
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(roomRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 0)
}
