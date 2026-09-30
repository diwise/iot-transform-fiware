package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/matryer/is"
)

// En komplett, realistisk konfiguration ska parsa och validera.
func TestParseModelFullExample(t *testing.T) {
	is := is.New(t)

	data := []byte(`
rules:
  - match: { kind: measurement, sensorType: waterquality }
    entities:
      - name: observed
        id: "urn:ngsi-ld:WaterQualityObserved:{{deviceID}}"
        type: WaterQualityObserved
        context: "https://example.invalid/context.jsonld"
        create: true
        properties:
          - target: temperature
            type: Number
            resource: "5700"
            unit: Cel
            transform: [{ op: multiply, value: 2 }]
          - target: category
            type: TextArray
            name: category
          - target: address
            type: Structured
            subproperties:
              - { target: streetAddress, type: Text, name: streetAddress }
      - name: beach
        id: "urn:ngsi-ld:Beach:{{safeURI(field:name)}}"
        type: Beach
        create: true
        properties:
          - { target: name, type: Text, name: name }
      - id: "urn:ngsi-ld:Thing:{{entities.observed.id}}"
        type: Thing
        properties:
          - { target: refObserved, type: Relationship, value: "{{entities.observed.id}}" }
`)

	rules, err := ParseModel(data)
	is.NoErr(err)
	is.Equal(len(rules), 1)
	is.NoErr(transform.Validate(rules))
}

// Valideringsfel ska propageras från LoadModelsDir.
func TestLoadModelsDirPropagatesValidationError(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	is.NoErr(os.WriteFile(filepath.Join(dir, "bad.yaml"),
		[]byte("rules:\n  - match: { kind: nope }\n    entities: []\n"), 0o644))

	_, err := LoadModelsDir(dir)
	is.True(err != nil)
}

// En tom katalog (inga regler) ska ge fel i stället för en tyst tom motor.
func TestLoadModelsDirRejectsEmptyDir(t *testing.T) {
	is := is.New(t)

	_, err := LoadModelsDir(t.TempDir())
	is.True(err != nil)
}

// En fil med okänt fält ska ge fel via LoadModelsDir.
func TestLoadModelsDirRejectsUnknownField(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	is.NoErr(os.WriteFile(filepath.Join(dir, "bad.yaml"),
		[]byte("rules:\n  - match: { kind: measurement }\n    entities: []\n    oops: 1\n"), 0o644))

	_, err := LoadModelsDir(dir)
	is.True(err != nil)
}

// Okända fält ska ge fel (strikt avkodning).
func TestParseModelRejectsUnknownField(t *testing.T) {
	is := is.New(t)

	_, err := ParseModel([]byte("rules:\n  - match: { kind: measurement }\n    entities: []\n    oops: 1\n"))
	is.True(err != nil)
}

// Filer läses i lexikografisk filnamnsordning och reglerna slås ihop i den
// ordningen.
func TestLoadModelsDirUsesLexicographicOrder(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	model := "rules:\n  - match: { kind: thing, event: things.v1.values, type: %s }\n    entities:\n      - id: urn:ngsi-ld:Thing:x\n        type: Thing\n        properties: []\n"
	is.NoErr(os.WriteFile(filepath.Join(dir, "a-first.yaml"), []byte(fmt.Sprintf(model, "First")), 0o644))
	is.NoErr(os.WriteFile(filepath.Join(dir, "b-second.yaml"), []byte(fmt.Sprintf(model, "Second")), 0o644))
	is.NoErr(os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("not yaml"), 0o644))

	rules, err := LoadModelsDir(dir)
	is.NoErr(err)
	is.Equal(len(rules), 2)
	is.Equal(rules[0].Match.Type, "First")
	is.Equal(rules[1].Match.Type, "Second")
}
