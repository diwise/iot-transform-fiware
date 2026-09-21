package transform

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// JSON-kontraktet för regel-API:t: YAML (seed/disk) -> struct -> JSON ->
// struct ska vara förlustfritt för samtliga skeppade modeller, så att
// API:t aldrig tappar fält som YAML-formen bär (särskilt any-fälten
// const/value samt citerade punktvägar).
func TestJSONRoundTripShippedModels(t *testing.T) {
	files, err := filepath.Glob("../../../deployments/models/*.yaml")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no shipped models found")
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		var m Model
		if err := yaml.Unmarshal(raw, &m); err != nil {
			t.Fatalf("%s: yaml: %v", f, err)
		}
		js, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("%s: json marshal: %v", f, err)
		}
		var back Model
		if err := json.Unmarshal(js, &back); err != nil {
			t.Fatalf("%s: json unmarshal: %v", f, err)
		}
		if !reflect.DeepEqual(m, back) {
			t.Fatalf("%s: YAML->JSON round trip lost fields", f)
		}
	}
}

// JSON-nycklar ska vara camelCase (plattformskonvention), aldrig Go-fältnamn.
func TestJSONKeysAreCamelCase(t *testing.T) {
	raw := `{"match":{"kind":"thing","sensorType":"x"},"entities":[]}`
	var r Rule
	dec := json.NewDecoder(strings.NewReader(raw))
	if err := dec.Decode(&r); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if r.Match.SensorType != "x" {
		t.Fatalf("sensorType not decoded: %+v", r.Match)
	}
	js, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(js), "SensorType") {
		t.Fatalf("Go field name leaked into JSON: %s", js)
	}
}
