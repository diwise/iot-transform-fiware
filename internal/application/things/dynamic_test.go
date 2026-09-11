package things

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

func marshalEntity(t *testing.T, id, typeName string, props []entities.EntityDecoratorFunc) string {
	t.Helper()

	e, err := entities.New(id, typeName, props...)
	if err != nil {
		t.Fatalf("entities.New: %v", err)
	}

	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	return string(b)
}

func envelope(t *testing.T, ts time.Time, thing any) []byte {
	t.Helper()

	b, err := json.Marshal(struct {
		Timestamp time.Time `json:"timestamp"`
		Thing     any       `json:"thing"`
	}{ts, thing})
	if err != nil {
		t.Fatalf("json.Marshal envelope: %v", err)
	}

	return b
}

const containerRules = `
rules:
  - match: { kind: thing, type: Container }
    entities:
      - id: "urn:ngsi-ld:{{type}}:{{nameOrID}}"
        type: "{{type}}"
        properties:
          - { target: fillingLevel, type: Number, field: percent, observedAt: header }
          - { target: location, type: Geo, field: location }
          - { target: dateObserved, type: DateTime, timestamp: true }
`

func TestContainerMatchesDynamicEngine(t *testing.T) {
	is := is.New(t)

	c := Container{
		Thing: Thing{
			ID: "2bf440f4", Type: "Container", SubType: new("WasteContainer"),
			Name: "Soptunnor.X", AlternativeName: "Soptunnor.XY",
			Location: testLocation(), ObservedAt: testTime(), Tenant: "default",
		},
		CurrentLevel: 0.91,
		Percent:      56,
	}

	static := TransformContainer(c)[0]

	th, err := ThingFromJSON(envelope(t, testTime(), c))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(containerRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	is.Equal(
		marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props),
		marshalEntity(t, static.EntityID, static.TypeName, static.Props),
	)
}
