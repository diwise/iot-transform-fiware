package things

import (
	"encoding/json"
	"strings"
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
          - { target: location, type: Geo, field: location, required: true }
`

func TestContainerIsModelConformant(t *testing.T) {
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

	th, err := ThingFromJSON(envelope(t, testTime(), c))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(containerRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:WasteContainer:Soptunnor.XY")
	is.Equal(writes[0].TypeName, "WasteContainer")

	b := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
	is.True(strings.Contains(b, `"fillingLevel":{"type":"Property","value":56`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
	is.True(!strings.Contains(b, `"dateObserved"`)) // WasteContainer saknar dateObserved
}

func TestContainerWithoutRequiredLocationIsSkipped(t *testing.T) {
	is := is.New(t)

	body := []byte(`{"thing":{"id":"x","type":"Container","subType":"WasteContainer","name":"n","percent":50,"tenant":"default"},"tenant":"default","timestamp":"2024-01-01T00:00:00Z"}`)
	th, err := ThingFromJSON(body)
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(containerRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 0)
}
