package config

import (
	"testing"

	"github.com/matryer/is"
)

// Fritextnamn på regeln ska accepteras av den strikta YAML-parsen och
// bevaras; det är endast till för användaren.
func TestParseModelAcceptsRuleName(t *testing.T) {
	is := is.New(t)

	rules, err := ParseModel([]byte(`
rules:
  - name: "Rumtemperatur till observerad enhet"
    match: { kind: measurement, object: "urn:oma:lwm2m:ext:3303" }
    entities:
      - id: "urn:ngsi-ld:Thing:x"
        type: Thing
        properties:
          - { target: temperature, type: Number, resource: temperature }
`))
	is.NoErr(err)
	is.Equal(len(rules), 1)
	is.Equal(rules[0].Name, "Rumtemperatur till observerad enhet")
}
